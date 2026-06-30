package br.gov.lexml.scala_restic.controller.impl

import br.gov.lexml.scala_restic.command.ResticCommandService
import zio.*
import zio.stream.*
import zio.config.*
import zio.config.magnolia.*
import br.gov.lexml.scala_restic.misc.ZioConfigInstances.given
import br.gov.lexml.scala_restic.controller.{GeneralRepoControllerException, ProcessData, RepoBackupData, RepoBackupException, RepoBackupItemException, RepoControllerException, RepoRestoreData, RepoRestoreException, RepoRestoreItemException, ResticRepoController}
import br.gov.lexml.scala_restic.data.backup.BackupMessage
import br.gov.lexml.scala_restic.data.init.InitResult
import br.gov.lexml.scala_restic.data.restore
import br.gov.lexml.scala_restic.data.restore.RestoreMessage
import br.gov.lexml.scala_restic.data.snapshots.Snapshot
import br.gov.lexml.scala_restic.options.backup.BackupOptions
import br.gov.lexml.scala_restic.options.common.{CommonOptions, Repo}
import br.gov.lexml.scala_restic.options.forget.ForgetOptions
import br.gov.lexml.scala_restic.options.restore.RestoreOptions
import br.gov.lexml.scala_restic.options.snapshots.SnapshotsOptions
import zio.config.derivation.kebabCase

import java.nio.file.Path



class ResticRepoControllerImpl(
  config : ResticRepoControllerImplConfig,
  resticCommandService : ResticCommandService) extends ResticRepoController:
  private val repo : Repo = Repo.R_LOCATION(config.repoPath.toString)
  override def repoExists(
    commonOptions: CommonOptions = CommonOptions()
  ): Task[Boolean] =
    resticCommandService.checkRepositoryExistence(
      repo = repo, commonOptions = config.makeCommonOptions(commonOptions), password = config.passwordOption
    )

  override def init(commonOptions: CommonOptions = CommonOptions()): Task[InitResult] =
    resticCommandService.init(
      repo = repo, commonOptions = config.makeCommonOptions(commonOptions), password = config.passwordOption
    )

  override def snapshots(
    commonOptions: CommonOptions = CommonOptions(),
    snapshotOptions : SnapshotsOptions = SnapshotsOptions()): Task[Vector[Snapshot]] =
    resticCommandService.snapshots(
      repo = repo,
      commonOptions = config.makeCommonOptions(commonOptions),
      snapshotsOptions = snapshotOptions,
      password = config.passwordOption
    ).map(_.snapshots)

  private final case class ProcessDataImpl[Summary,Status](
    fiber : Fiber[RepoControllerException,Summary],
    statusHub : Hub[Take[Nothing, Status]],
    completed : Promise[Nothing, Unit],
  ) extends ProcessData[Summary,Status]:
    override def statusStream: ZStream[Any, Nothing, Status] =
      ZStream.unwrapScoped {
        completed.isDone.flatMap {
          case true => ZIO.succeed(ZStream.empty)
          case false =>
            statusHub.subscribe.flatMap { subscription =>
              completed.isDone.map {
                case true => ZStream.empty
                case false => ZStream.fromQueue(subscription).flattenTake
              }
            }
        }
      }

    override def awaitSummary: ZIO[Any, RepoControllerException, Summary] =
      fiber.join

    override def cancel: UIO[Unit] =
      fiber.interrupt.unit

  override def restore(snapshotID: String = "latest", commonOptions: CommonOptions, restoreOptions: RestoreOptions): ZIO[Scope, RepoControllerException, RepoRestoreData] =
    for {
      restoreStream <- resticCommandService.restoreStream(
        repo = repo,
        commonOptions = config.makeCommonOptions(commonOptions),
        restoreOptions = config.makeRestoreOptions(restoreOptions),
        password = config.passwordOption,
        snapshotID = snapshotID
      ).mapError { err => RepoRestoreException(s"Error during restic restore execution: ${err.getMessage}",Cause.fail(err)) }
      statusHub <- ZIO.acquireRelease(Hub.sliding[Take[Nothing, RestoreMessage.Status]](100))(_.shutdown)
      completed <- Promise.make[Nothing, Unit]
      restoreFiber <- (
        for {
          (optSummary,error) <-
            restoreStream
              .mapError { err => RepoRestoreException(s"Error during restic restore execution: ${err.getMessage}", Cause.fail(err)) }
              .either
              .runFoldZIO[Any,Nothing,(Option[RestoreMessage.Summary],Cause[RepoControllerException])]((None,Cause.empty)) {
                case ((optSummary,errors),Right(msg)) => msg match {
                  case msg: RestoreMessage.Status =>
                    statusHub.publish(Take.single(msg)).as((optSummary,errors))
                  case msg: RestoreMessage.Summary => ZIO.succeed((Some(msg),errors))
                  case msg: RestoreMessage.Error =>
                    ZIO.succeed((optSummary,
                      errors ++ Cause.fail(RepoRestoreItemException(msg))))
                  case _ => ZIO.succeed((optSummary,errors))
                }
                case ((optSummary,errors),Left(streamError)) =>
                  ZIO.succeed((optSummary,errors ++ Cause.fail(streamError)))
              }
          _ <- ZIO.when(error.nonEmpty) {
            ZIO.failCause(error)
          }
          summary <- ZIO.fromOption(optSummary).mapError(_ => GeneralRepoControllerException(s"Summary not found during restore!"))
        } yield summary
      ).ensuring(completed.succeed(()) *> statusHub.publish(Take.end).unit).forkScoped
      restoreProcess = ProcessDataImpl(restoreFiber, statusHub, completed)
      _ <- ZIO.addFinalizer(restoreProcess.cancel)
    } yield restoreProcess

  
  override def backup(commonOptions: CommonOptions, backupOptions: BackupOptions): ZIO[Scope, RepoControllerException, RepoBackupData] =
    for {
      backupStream <- resticCommandService.backupStream(
        repo = repo,
        commonOptions = config.makeCommonOptions(commonOptions),
        backupOptions = config.makeBackupOptions(backupOptions),
        password = config.passwordOption,
        basePath = config.backupRestoreBaseDir,
        paths = config.paths,
      ).mapError { err => RepoBackupException(s"Error during restic backup execution: ${err.getMessage}", Cause.fail(err)) }
      statusHub <- ZIO.acquireRelease(Hub.sliding[Take[Nothing, BackupMessage.Status]](100))(_.shutdown)
      completed <- Promise.make[Nothing, Unit]
      backupFiber <- (
        for {
          (optSummary, error) <- backupStream
            .mapError(ex => RepoBackupException(s"Error during restic backup execution",Cause.fail(ex)))
            .either
            .runFoldZIO[Any, Nothing, (Option[BackupMessage.Summary], Cause[RepoControllerException])]((None, Cause.empty)) {
              case ((optSummary, errors), Right(msg)) => msg match {
                case msg: BackupMessage.Status =>
                  statusHub.publish(Take.single(msg)).as((optSummary, errors))
                case msg: BackupMessage.Summary => ZIO.succeed((Some(msg), errors))
                case msg: BackupMessage.Error =>
                  ZIO.succeed((optSummary,
                    errors ++ Cause.fail(RepoBackupItemException(msg))))
                case _ => ZIO.succeed((optSummary, errors))
              }
              case ((optSummary, errors), Left(streamError)) =>
                ZIO.succeed((optSummary, errors ++ Cause.fail(streamError)))
            }
          _ <- ZIO.when(error.nonEmpty) {
            ZIO.failCause(error)
          }
          summary <- ZIO.fromOption(optSummary).mapError(_ => GeneralRepoControllerException(s"Summary not found during backup!"))
        } yield summary
        ).ensuring(completed.succeed(()) *> statusHub.publish(Take.end).unit).forkScoped
      backupProcess = ProcessDataImpl(backupFiber, statusHub, completed)
      _ <- ZIO.addFinalizer(backupProcess.cancel)
    } yield backupProcess

  override def forget(commonOptions : CommonOptions = CommonOptions(), forgetOptions : ForgetOptions = ForgetOptions()) : Task[Unit] =
    resticCommandService.forget(
      repo = repo,
      commonOptions = config.makeCommonOptions(commonOptions),
      forgetOptions = config.makeForgetOptions(forgetOptions),
      password = config.passwordOption
    )


object ResticRepoControllerImpl:
  def make(name : String) : ZIO[Scope & ResticCommandService,Throwable,ResticRepoController] =
    for {
      resticCommandService <- ZIO.service[ResticCommandService]
      config <-
        ZIO.config(ResticRepoControllerImplConfigs.config).map(_.controllers.get(name))
          .someOrFail(new Exception(s"Restic repo configuration not found for repo named: $name"))
    } yield ResticRepoControllerImpl(config, resticCommandService)

  def layer(name : String) : ZLayer[ResticCommandService,Throwable,ResticRepoController] =
    ZLayer.scoped(make(name))
