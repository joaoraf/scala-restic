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
  backupDirs : Vector[Path] = Vector(),
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
      .map { l => ResticRepoControllerImplConfigs(l.groupBy(_.name).view.mapValues(_.head).toMap) }

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
  ) extends RepoRestoreData {
    override def statusDequeue: Dequeue[RestoreMessage.Status] = statusQueue
    override def awaitSummary: ZIO[Any,RepoControllerException,RestoreMessage.Summary] =
      restoreFiber.join.catchAllCause { cause =>
        if cause.isInterruptedOnly then ZIO.fail(RepoRestoreException("Restore interrupted."))
        else ZIO.fail(RepoRestoreException("Error during restore",cause))
      }
    override def cancel: Task[Unit] =
      restoreFiber.interrupt.ignore
  }

  override def restore(snapshotID: String = "latest", commonOptions: CommonOptions, restoreOptions: RestoreOptions): ZIO[Scope, RepoControllerException, RepoRestoreData] =
    for {
      restoreStream <- resticCommandService.restoreStream(
        repo = repo,
        commonOptions = config.makeCommonOptions(commonOptions),
        restoreOptions = config.makeRestoreOptions(restoreOptions),
        password = config.passwordOption,
        snapshotID = snapshotID
      ).mapErrorCause { err => Cause.fail(RepoRestoreException(s"Error during restic restore execution: ${err.prettyPrint}",err)) }
      statusQueue <- Queue.sliding[RestoreMessage.Status](100)
      restoreFiber <- (
        for {
          (optSummary,error) <- restoreStream.runFoldZIO[Any,Exception,(Option[RestoreMessage.Summary],Cause[RepoRestoreItemException])]((None,Cause.empty)) {
            case ((optSummary,errors),msg) => msg match {
              case msg: RestoreMessage.Status => statusQueue.offer(msg) *> ZIO.succeed((optSummary,errors))
              case msg: RestoreMessage.Summary => ZIO.succeed((Some(msg),errors))
              case msg: RestoreMessage.Error =>
                ZIO.succeed((optSummary,
                  errors ++ Cause.fail(RepoRestoreItemException(msg))))
              case _ => ZIO.succeed((optSummary,errors))
            }
          }.catchAllCause { cause => ZIO.succeed((None,Cause.fail(GeneralRepoControllerException(s"Failure during restore",cause)))) }
          _ <- ZIO.when(error.nonEmpty) {
            ZIO.failCause(error)
          }
          summary <- ZIO.fromOption(optSummary).mapError(_ => GeneralRepoControllerException(s"Summary not found during restore!"))
        } yield summary
      ).forkScoped
      _ <- ZIO.addFinalizer(restoreFiber.interrupt.ignore)
    } yield RestoreProcess(restoreFiber, statusQueue)

  private final case class BackupProcess(
    backupFiber: Fiber[RepoControllerException, BackupMessage.Summary],
    statusQueue: Queue[BackupMessage.Status],
  ) extends RepoBackupData {
    override def statusDequeue: Dequeue[BackupMessage.Status] = statusQueue

    override def awaitSummary: ZIO[Any, RepoControllerException, BackupMessage.Summary] =
      backupFiber.join.catchAllCause { cause =>
        if cause.isInterruptedOnly then ZIO.fail(RepoBackupException("Backup interrupted."))
        else ZIO.fail(RepoBackupException("Error during backup", cause))
      }

    override def cancel: Task[Unit] =
      backupFiber.interrupt.ignore
  }
  
  override def backup(baseDir: Path, paths: NonEmptyChunk[Path], commonOptions: CommonOptions, backupOptions: BackupOptions): ZIO[Scope, RepoControllerException, RepoBackupData] =
    for {
      backupStream <- resticCommandService.backupStream(
        repo = repo,
        commonOptions = config.makeCommonOptions(commonOptions),
        backupOptions = config.makeBackupOptions(backupOptions),
        password = config.passwordOption,
        basePath = config.backupRestoreBaseDir,
        paths = paths,
      ).mapErrorCause { err => Cause.fail(RepoBackupException(s"Error during restic backup execution: ${err.prettyPrint}", err)) }
      statusQueue <- Queue.sliding[BackupMessage.Status](100)
      backupFiber <- (
        for {
          (optSummary, error) <- backupStream.runFoldZIO[Any, Exception, (Option[BackupMessage.Summary], Cause[RepoBackupItemException])]((None, Cause.empty)) {
            case ((optSummary, errors), msg) => msg match {
              case msg: BackupMessage.Status => statusQueue.offer(msg) *> ZIO.succeed((optSummary, errors))
              case msg: BackupMessage.Summary => ZIO.succeed((Some(msg), errors))
              case msg: BackupMessage.Error =>
                ZIO.succeed((optSummary,
                  errors ++ Cause.fail(RepoBackupItemException(msg))))
              case _ => ZIO.succeed((optSummary, errors))
            }
          }.catchAllCause { cause => ZIO.succeed((None, Cause.fail(GeneralRepoControllerException(s"Failure during backup", cause)))) }
          _ <- ZIO.when(error.nonEmpty) {
            ZIO.failCause(error)
          }
          summary <- ZIO.fromOption(optSummary).mapError(_ => GeneralRepoControllerException(s"Summary not found during backup!"))
        } yield summary
        ).forkScoped
      _ <- ZIO.addFinalizer(backupFiber.interrupt.ignore)
    } yield BackupProcess(backupFiber, statusQueue)

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


