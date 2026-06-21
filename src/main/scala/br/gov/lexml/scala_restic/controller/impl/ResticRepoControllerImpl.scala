package br.gov.lexml.scala_restic.controller.impl

import br.gov.lexml.scala_restic.command.ResticCommandService
import br.gov.lexml.scala_restic.controller.{BackupRestoreController, ResticRepoController}
import br.gov.lexml.scala_restic.controller.impl.BackupRestoreControllerState.BRC_IDLE
import br.gov.lexml.scala_restic.data.backup.BackupMessage
import br.gov.lexml.scala_restic.data.restore.RestoreMessage
import br.gov.lexml.scala_restic.options.backup.BackupOptions
import br.gov.lexml.scala_restic.options.common.{CommonOptions, Repo}
import br.gov.lexml.scala_restic.options.restore.RestoreOptions
import br.gov.lexml.scala_restic.options.snapshots.SnapshotsOptions
import zio.*
import zio.config.*
import zio.config.magnolia.*

import java.time.{OffsetDateTime, Duration as JavaDuration}
import java.nio.file.{Files, Path}
import cron4s.*
import cron4s.lib.javatime.*

final case class ResticRepoControllerConfig(
  resticExecutablePath : Path = Path.of("/usr/bin/restic"),
  hostName : String = "localhost",
  backupRestoreControllers : Vector[BackupRestoreControllerConfig] = Vector()
)

object ResticRepoControllerConfig:
  import br.gov.lexml.scala_restic.misc.ZioConfigInstances.given
  val config: Config[ResticRepoControllerConfig] = deriveConfig[ResticRepoControllerConfig]

final case class ResticRepoConfig(
  repoPath : Path,
  repoPassword : String = "",
  host : String
)
object ResticRepoConfig:
  import br.gov.lexml.scala_restic.misc.ZioConfigInstances.given
  val config: Config[ResticRepoConfig] = deriveConfig[ResticRepoConfig]

final case class BRC_BackupOptions(
  excludes : List[String] = List(),
  iexcludes : List[String] = List(),
  includes : List[String] = List(),
  iincludes : List[String] = List(),
  tags : List[String] = List(),
  oneFileSystem : Boolean = false,
  readConcurrency : Int = 0,
  skipIfChanged : Boolean = false,
  host : String = ""
)

object BRC_BackupOptions:
  import br.gov.lexml.scala_restic.misc.ZioConfigInstances.given
  val config: Config[BRC_BackupOptions] = deriveConfig[BRC_BackupOptions]

final case class BRC_RestoreOptions(
  excludes : List[String] = List(),
  iexcludes : List[String] = List(),
  includes : List[String] = List(),
  iincludes : List[String] = List(),
  tags : List[String] = List(),
  deleteExcluded : Boolean = false,
  hosts : List[String] = List()
)

object BRC_RestoreOptions:
  import br.gov.lexml.scala_restic.misc.ZioConfigInstances.given
  val config: Config[BRC_RestoreOptions] = deriveConfig[BRC_RestoreOptions]

final case class BackupRestoreControllerConfig(
  resticRepo : ResticRepoConfig,
  initIfNecessary : Boolean = true,
  restoreIfEmpty : Boolean = true,
  backupCronSchedule : CronExpr,
  backupFolders : NonEmptyChunk[Path],
  restoreOptions : BRC_RestoreOptions = BRC_RestoreOptions(),
  backupOptions : BRC_BackupOptions = BRC_BackupOptions()
)

object BackupRestoreControllerConfig:
  import br.gov.lexml.scala_restic.misc.ZioConfigInstances.given
  val config: Config[BackupRestoreControllerConfig] = deriveConfig[BackupRestoreControllerConfig]


enum BackupRestoreControllerState:
  case BRC_STARTING
  case BRC_INITIAL_RESTORE(
    restoreFiberPromise : Promise[Nothing,Fiber[Throwable,Unit]]
  )
  case BRC_IDLE(
    commandPromise : Promise[Nothing,Unit],
    commandFiberPromise : Promise[Nothing,Fiber[Throwable,Unit]],
    optWaitingFiberNexTime : Option[(Fiber[Throwable,Unit],OffsetDateTime)]
    )
  case BRC_BACKING_UP(backupFiberPromise : Promise[Nothing,Fiber[Throwable,Unit]])
  case BRC_FAILED(when : OffsetDateTime, ex : Option[Throwable] = None, message : Option[String] = None)

enum BackupRestoreEvent:
  case BRE_STARTED
  case BRE_RESTORING
  case BRE_RESTORING_STATUS(status : RestoreMessage.Status)
  case BRE_RESTORING_ERROR(status : RestoreMessage.Error)
  case BRE_RESTORED(summary : RestoreMessage.Summary)
  case BRE_BACKING_UP
  case BRE_BACKING_UP_STATUS(status: BackupMessage.Status)
  case BRE_BACKING_UP_ERROR(status: BackupMessage.Error)
  case BRE_BACKED_UP(summary: BackupMessage.Summary)
  case BRE_WAITING
  case BRE_FAILED(ex : Option[Throwable] = None, message : Option[String] = None)

class BackupRestoreControllerImpl(
  config : BackupRestoreControllerConfig,
  state : Ref[BackupRestoreControllerState],
  eventHub : Hub[BackupRestoreEvent],
  resticCommandService : ResticCommandService) extends BackupRestoreController:
  private val repo = Repo.atFolder(config.resticRepo.repoPath)
  private val password = Some(config.resticRepo.repoPassword).filter(x => !x.isBlank)

  private val backupSchedule : Schedule[Any,Any, OffsetDateTime] =
    new Schedule[Any, Any, OffsetDateTime]:
      override type State = Unit
      override val initial: State = ()

      override def step(now: OffsetDateTime, in: Any, state: State)(using
        Trace
      ): UIO[(State, OffsetDateTime, Schedule.Decision)] =
        config.backupCronSchedule.next(now) match
          case Some(nextRun) =>
            ZIO.succeed(((), nextRun, Schedule.Decision.Continue(Schedule.Interval.after(nextRun))))
          case None =>
            ZIO.succeed(((), OffsetDateTime.MAX, Schedule.Decision.Done))

  private inline def doesRepoPathExists : Task[Boolean] =
    ZIO.attempt(Files.exists(config.resticRepo.repoPath))

  def start : Task[Unit] =
    eventHub.publish(BackupRestoreEvent.BRE_STARTED) *>
    ZIO.ifZIO(doesRepoPathExists)(startWithExistingPath(),startWithInitialization())


  private inline def startWithExistingPath(): Task[Unit] =
      ZIO.ifZIO(resticCommandService.checkRepositoryExistence(repo, password = password))(
          startWithExistingRepo(),
          ZIO.fail(new Exception(s"Repository path exists but is not a valid restic repository, repoPath=${config.resticRepo.repoPath}"))
        )

  private inline def startWithExistingRepo(): Task[Unit] = {
    for {
      ss <- resticCommandService.snapshots(repo,password=password, snapshotsOptions = SnapshotsOptions(latest=1))
      _ <- if ss.snapshots.isEmpty then startWithoutSnapshots() else startWithSnapshots(ss.snapshots.head.id)
    }  yield ()
  }

  private inline def startWithoutSnapshots(): Task[Unit] = goIdle

  private inline def startWithSnapshots(snapshotID : String): Task[Unit] =
      ZIO.ifZIO(ZIO.exists(config.backupFolders)(folder => ZIO.attempt(Files.exists(folder))))(
        goIdle,
        if config.restoreIfEmpty then startWithInitialRestore(snapshotID) else goIdle
      )

  private inline def startWithInitialRestore(snapshotID : String): Task[Unit] = {
    val restoreOptions = RestoreOptions(
      exclude = config.restoreOptions.excludes,
      iexclude = config.restoreOptions.iexcludes,
      include = config.restoreOptions.includes,
      iinclude = config.restoreOptions.iincludes,
      tag = config.restoreOptions.tags,
      delete = config.restoreOptions.deleteExcluded,
      host = config.restoreOptions.hosts
    )
    for {
      restoreStream <- resticCommandService.restoreStream(repo,restoreOptions=restoreOptions, password=password, snapshotID = snapshotID)
      restoreFiberPromise <- Promise.make[Nothing,Fiber[Throwable,Unit]]
      _ <- setControllerState(BackupRestoreControllerState.BRC_INITIAL_RESTORE(restoreFiberPromise))
      _ <- eventHub.publish(BackupRestoreEvent.BRE_RESTORING)
      restoreFiber <- restoreStream.runForeach {
          case msg : RestoreMessage.Status =>
            eventHub.publish(BackupRestoreEvent.BRE_RESTORING_STATUS(msg))
          case msg : RestoreMessage.VerboseStatus => ZIO.unit
          case msg : RestoreMessage.Summary =>
            eventHub.publish(BackupRestoreEvent.BRE_RESTORED(msg))
          case msg : RestoreMessage.Error =>
            eventHub.publish(BackupRestoreEvent.BRE_RESTORING_ERROR(msg))
        }
        .flatMap {_ => goIdle }
        .catchAll(err => goFailure(ex = Some(err), message = Some(s"Error during restore: ${err.getMessage}")))
        .forkDaemon
      _ <- restoreFiberPromise.succeed(restoreFiber)
    } yield ()
  }

  private def goFailure(ex : Option[Throwable] = None, message : Option[String] = None) =
    for {
      now <- ZIO.clockWith(_.currentDateTime)
      _ <- state.set(BackupRestoreControllerState.BRC_FAILED(now,ex = ex,message = message))
      _ <- eventHub.publish(BackupRestoreEvent.BRE_FAILED(ex,message))
    } yield ()


  private def setControllerState(newState : BackupRestoreControllerState): Task[Unit] =
    state.set(newState)

  private def onceAt(at: OffsetDateTime): Schedule[Any, Any, Unit] =
    Schedule.once.addDelayZIO { _ =>
      Clock.currentDateTime.map { now =>
        val current = now.toInstant
        val target  = at.toInstant

        if (target.isAfter(current))
          Duration.fromInterval(current, target)
        else
          Duration.Zero
      }
    }

  private inline def cleanUpIdle : UIO[Unit] = {
    state.get.flatMap {
      case BRC_IDLE(commandPromise,commandFiberPromise,optWaitingFiberNextTime) =>
        for {
          _ <- commandFiberPromise.await.flatMap(_.interrupt)
          _ <- optWaitingFiberNextTime match {
            case Some((fiber, _)) => fiber.interrupt
            case None => ZIO.unit
          }
        } yield ()
      case _ => ZIO.unit
    }
  }

  private def goIdle : Task[Unit] = for {
    commandPromise <- Promise.make[Nothing,Unit]
    optWaitingFiberNextTime <- for {
      now <- ZIO.clockWith(_.currentDateTime)
      fiber <- config.backupCronSchedule.next(now) match {
        case Some(nextRun) => commandPromise.succeed(()).schedule(onceAt(nextRun)).forkDaemon.map(f => Some(f,nextRun))
        case None => ZIO.succeed(None)
      }
    } yield fiber
    commandFiberPromise <- Promise.make[Nothing,Fiber[Throwable,Unit]]
    _ <- setControllerState(BackupRestoreControllerState.BRC_IDLE(commandPromise,commandFiberPromise,optWaitingFiberNextTime))
    commandFiber <- (commandPromise.await *> doBackup).forkDaemon
    _ <- commandFiberPromise.succeed(commandFiber).ignore
  } yield ()

  private def doBackup : Task[Unit] = {
    val backupOptions = BackupOptions(
      exclude = config.backupOptions.excludes,
      iexclude = config.backupOptions.iexcludes,
      tag = config.backupOptions.tags,
      oneFileSystem = config.backupOptions.oneFileSystem,
      readConcurrency = Some(config.backupOptions.readConcurrency).filter(_ > 0),
      skipIfChanged = config.backupOptions.skipIfChanged,
    )
    for {
      backupStream <- resticCommandService.backupStream(
        repo = repo,
        backupOptions = backupOptions,
        password = password,
        paths = config.backupFolders
      )
      backupFiberPromise <- Promise.make[Nothing,Fiber[Throwable,Unit]]
      _ <- setControllerState(BackupRestoreControllerState.BRC_BACKING_UP(backupFiberPromise))
      _ <- eventHub.publish(BackupRestoreEvent.BRE_BACKING_UP)
      backupFiber <- backupStream.runForeach {
          case msg : BackupMessage.Status =>
            eventHub.publish(BackupRestoreEvent.BRE_BACKING_UP_STATUS(msg))
          case msg : BackupMessage.VerboseStatus => ZIO.unit
          case msg : BackupMessage.Summary =>
            eventHub.publish(BackupRestoreEvent.BRE_BACKED_UP(msg))
          case msg : BackupMessage.Error =>
            eventHub.publish(BackupRestoreEvent.BRE_BACKING_UP_ERROR(msg))
        }
        .ensuring(cleanUpIdle)
        .foldZIO(
          err => goFailure(ex = Some(err), message = Some(s"Error during backup: ${err.getMessage}")),
          _ => goIdle
        ).forkDaemon
      _ <- backupFiberPromise.succeed(backupFiber)
    } yield ()
  }

  private inline def startWithInitialization() : Task[Unit] =
    ZIO.when(config.initIfNecessary)(resticCommandService.init(repo, password = password)) *> goIdle

  def backupNow: UIO[Boolean] =
    state.get.flatMap {
      case BRC_IDLE(commandPromise,_,_) => commandPromise.succeed(())
      case _ => ZIO.logWarning("Ignoring backupNow() call because the controller is not idle") *> ZIO.succeed(false)
    }

  def subscribe: ZIO[Scope, Nothing, Dequeue[BackupRestoreEvent]] = eventHub.subscribe
