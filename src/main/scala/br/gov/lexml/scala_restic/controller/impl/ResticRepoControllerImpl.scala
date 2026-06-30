package br.gov.lexml.scala_restic.controller.impl

import br.gov.lexml.scala_restic.command.ResticCommandService
import zio.*
import zio.stream.*
import zio.config.*
import zio.config.magnolia.*
import br.gov.lexml.scala_restic.misc.ZioConfigInstances.given
import br.gov.lexml.scala_restic.controller.{GeneralRepoControllerException, RepoBackupData, RepoBackupException, RepoBackupItemException, RepoControllerException, RepoRestoreData, RepoRestoreException, RepoRestoreItemException, ResticRepoController}
import br.gov.lexml.scala_restic.data.backup.BackupMessage
import br.gov.lexml.scala_restic.data.init.InitResult
import br.gov.lexml.scala_restic.data.restore
import br.gov.lexml.scala_restic.data.restore.RestoreMessage
import br.gov.lexml.scala_restic.data.snapshots.Snapshot
import br.gov.lexml.scala_restic.options.backup.BackupOptions
import br.gov.lexml.scala_restic.options.common.{CommonOptions, Repo}
import br.gov.lexml.scala_restic.options.restore.RestoreOptions
import br.gov.lexml.scala_restic.options.snapshots.SnapshotsOptions

import java.nio.file.Path

final case class ResticRepoControllerImplConfig(
  name : String,
  repoPath : Path,
  backupRestoreBaseDir : Path,
  paths : NonEmptyChunk[Path],
  password : String = "",
  passwordFile : Option[Path] = None,
  host : String = "",
  backupSkipIfUnchanged : Boolean = true,
  readConcurrency : Int = 0,
  deleteAfterRestore : Boolean = true,
):
  def makeCommonOptions(commonOptions : CommonOptions): CommonOptions =
    commonOptions.copy(
      insecureNoPassword = passwordOption.isEmpty && passwordFile.isEmpty,
      passwordFile = passwordFile,
      verbose = 1,
      json = true
    )

  def makeBackupOptions(backupOptions : BackupOptions) : BackupOptions =
    backupOptions.copy(
      host = Option(host).filter(x => !x.isBlank),
      skipIfUnchanged = backupSkipIfUnchanged,
      readConcurrency = Option(readConcurrency).filter(_ > 0).orElse(backupOptions.readConcurrency)
    )

  def makeRestoreOptions(restoreOptions : RestoreOptions) : RestoreOptions =
    restoreOptions.copy(
      delete = deleteAfterRestore,
      host = List(host).filter(x => !x.isBlank),
      target = Some(backupRestoreBaseDir),
    )

  def passwordOption : Option[String] = Option(password).filter(x => !x.isBlank)


object ResticRepoControllerImplConfig:
  val config: Config[ResticRepoControllerImplConfig] = deriveConfig[ResticRepoControllerImplConfig]


final case class ResticRepoControllerImplConfigs(
  controllers : Map[String,ResticRepoControllerImplConfig]
)

object ResticRepoControllerImplConfigs:
  val config : Config[ResticRepoControllerImplConfigs] =
    Config.listOf(ResticRepoControllerImplConfig.config).nested("restic","controllers")
      .mapOrFail { l =>
        val cm = l.groupBy(_.name)
        val ambiguous = cm.values.filter(_.length > 1).map(_.map(_.name).toSet).toSet.flatten
        if ambiguous.nonEmpty then
          Left(Config.Error.InvalidData(Chunk("name"),s"Multiple controller configs with the same name: ${ambiguous.mkString(", ")}"))
        else
          Right(ResticRepoControllerImplConfigs(cm.view.mapValues(_.head).toMap))
      }

class ResticRepoControllerImpl(
  config : ResticRepoControllerImplConfig,
  resticCommandService : ResticCommandService) extends ResticRepoController:
  private val repo : Repo = Repo.R_FILE(config.repoPath)
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

  private final case class RestoreProcess(
    restoreFiber : Fiber[RepoControllerException,RestoreMessage.Summary],
    statusQueue : Queue[RestoreMessage.Status],
  ) extends RepoRestoreData:
    override def statusStream: ZStream[Any,Nothing,RestoreMessage.Status] = ZStream.fromQueue(statusQueue)
    override def awaitSummary: ZIO[Any,RepoControllerException,RestoreMessage.Summary] =
      restoreFiber.join
    override def cancel: UIO[Unit] =
      restoreFiber.interrupt.ignore *> statusQueue.shutdown *> ZIO.succeed(true)

  end RestoreProcess

  override def restore(snapshotID: String = "latest", commonOptions: CommonOptions, restoreOptions: RestoreOptions): ZIO[Scope, RepoControllerException, RepoRestoreData] =
    for {
      restoreStream <- resticCommandService.restoreStream(
        repo = repo,
        commonOptions = config.makeCommonOptions(commonOptions),
        restoreOptions = config.makeRestoreOptions(restoreOptions),
        password = config.passwordOption,
        snapshotID = snapshotID
      ).mapError { err => RepoRestoreException(s"Error during restic restore execution: ${err.getMessage}",Cause.fail(err)) }
      statusQueue <- Queue.sliding[RestoreMessage.Status](100)
      restoreFiber <- (
        for {
          (optSummary,error) <-
            restoreStream
              .mapError { err => RepoRestoreException(s"Error during restic restore execution: ${err.getMessage}", Cause.fail(err)) }
              .runFoldZIO[Any,RepoControllerException,(Option[RestoreMessage.Summary],Cause[RepoRestoreItemException])]((None,Cause.empty)) {
            case ((optSummary,errors),msg) => msg match {
              case msg: RestoreMessage.Status => statusQueue.offer(msg) *> ZIO.succeed((optSummary,errors))
              case msg: RestoreMessage.Summary => ZIO.succeed((Some(msg),errors))
              case msg: RestoreMessage.Error =>
                ZIO.succeed((optSummary,
                  errors ++ Cause.fail(RepoRestoreItemException(msg))))
              case _ => ZIO.succeed((optSummary,errors))
            }
          }.catchAllCause { cause => ZIO.succeed((None,cause)) }.ensuring(statusQueue.shutdown)
          _ <- ZIO.when(error.nonEmpty) {
            ZIO.failCause(error)
          }
          summary <- ZIO.fromOption(optSummary).mapError(_ => GeneralRepoControllerException(s"Summary not found during restore!"))
        } yield summary
      ).forkScoped
      restoreProcess = RestoreProcess(restoreFiber, statusQueue)
      _ <- ZIO.addFinalizer(restoreProcess.cancel)
    } yield restoreProcess

  private final case class BackupProcess(
    backupFiber: Fiber[RepoControllerException, BackupMessage.Summary],
    statusQueue: Queue[BackupMessage.Status],
  ) extends RepoBackupData:
    override def statusStream: ZStream[Any,Nothing,BackupMessage.Status] = ZStream.fromQueue(statusQueue)

    override def awaitSummary: ZIO[Any, RepoControllerException, BackupMessage.Summary] =
      backupFiber.join

    override def cancel: UIO[Unit] =
      backupFiber.interrupt.ignore *> statusQueue.shutdown *> ZIO.succeed(true)

  end BackupProcess

  
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
      statusQueue <- ZIO.acquireRelease(Queue.sliding[BackupMessage.Status](100))(_.shutdown)
      backupFiber <- (
        for {
          (optSummary, error) <- backupStream
            .mapError(ex => RepoBackupException(s"Error during restic backup execution",Cause.fail(ex)))
            .runFoldZIO[Any, RepoBackupException, (Option[BackupMessage.Summary], Cause[RepoBackupItemException])]((None, Cause.empty)) {
            case ((optSummary, errors), msg) => msg match {
              case msg: BackupMessage.Status => statusQueue.offer(msg) *> ZIO.succeed((optSummary, errors))
              case msg: BackupMessage.Summary => ZIO.succeed((Some(msg), errors))
              case msg: BackupMessage.Error =>
                ZIO.succeed((optSummary,
                  errors ++ Cause.fail(RepoBackupItemException(msg))))
              case _ => ZIO.succeed((optSummary, errors))
            }
          }.catchAllCause { cause => ZIO.succeed((None, cause)) }.ensuring(statusQueue.shutdown)
          _ <- ZIO.when(error.nonEmpty) {
            ZIO.failCause(error)
          }
          summary <- ZIO.fromOption(optSummary).mapError(_ => GeneralRepoControllerException(s"Summary not found during backup!"))
        } yield summary
        ).forkScoped
      backupProcess = BackupProcess(backupFiber, statusQueue)
      _ <- ZIO.addFinalizer(backupProcess.cancel)
    } yield backupProcess

object ResticRepoControllerImpl:
  def layer(name : String) : ZLayer[ResticCommandService,Throwable,ResticRepoControllerImpl] =
    ZLayer.scoped {
      for {
        resticCommandService <- ZIO.service[ResticCommandService]
        config <-
          ZIO.config(ResticRepoControllerImplConfigs.config).map(_.controllers.get(name))
             .someOrFail(new Exception(s"Restic repo configuration not found for repo named: $name"))
      } yield ResticRepoControllerImpl(config,resticCommandService)
    }


