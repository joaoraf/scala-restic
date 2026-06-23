package br.gov.lexml.scala_restic.controller.impl

import zio.*
import zio.stream.*
import br.gov.lexml.scala_restic.command.ResticCommandService
import br.gov.lexml.scala_restic.controller.{BackupRestoreController, BackupRestoreControllers}
import br.gov.lexml.scala_restic.controller.BackupRestoreController.{BackupRequestResult, BackupRestoreHistory, State}
import br.gov.lexml.scala_restic.controller.impl.BackupRestoreControllerImpl.Command
import br.gov.lexml.scala_restic.data.backup.BackupMessage
import br.gov.lexml.scala_restic.data.restore.RestoreMessage
import br.gov.lexml.scala_restic.options.backup.BackupOptions
import br.gov.lexml.scala_restic.options.common.Repo
import br.gov.lexml.scala_restic.options.restore.RestoreOptions
import br.gov.lexml.scala_restic.options.snapshots.SnapshotsOptions
import cron4s.*
import cron4s.lib.javatime.*

import scala.collection.immutable.Queue as SQueue
import java.nio.file.{Files, Path}

private class BackupRestoreControllerImpl(
  config: BackupRestoreControllerConfig,
  commandQueue: Queue[BackupRestoreControllerImpl.Command],
  state: SubscriptionRef[BackupRestoreController.State],
  history: SubscriptionRef[BackupRestoreController.BackupRestoreHistory],
  commandFiberPromise: Promise[Nothing, Fiber[Throwable, Unit]],
  resticCommandService: ResticCommandService,
) extends BackupRestoreController:

  import BackupRestoreControllerImpl.*
  import State.*
  import Command.*

  private val repo = Repo.atFolder(config.resticRepo.repoPath)
  private val password = Option(config.resticRepo.repoPassword).filterNot(_.isBlank)

  private def insertError(cause: Cause[Throwable]): UIO[Unit] =
    commandQueue.offer(Error(cause)).unit


  /**
   * Initialize the controller. Checks the existence of the repository.
   * If the repository does not exist and the configuration allows initialization,
   * enqueue the Initialize command, otherwise, enqueue an error command.
   * If the repository exists, enqueue the restore command.
   */
  private def startController: Task[Unit] =
    for
      pathExists <- ZIO.attempt(Files.exists(config.resticRepo.repoPath))
      _ <-
        if !pathExists then
          if config.initIfNecessary then commandQueue.offer(Initialize).unit
          else ZIO.fail(new IllegalStateException(
            s"Repository does not exist and initialization is disabled: ${config.resticRepo.repoPath}"
          ))
        else
          resticCommandService.checkRepositoryExistence(repo, password = password).flatMap {
            case true => commandQueue.offer(Restore).unit
            case false => ZIO.fail(new IllegalStateException(
              s"Repository path exists but is not a valid restic repository: ${config.resticRepo.repoPath}"
            ))
          }
    yield ()

  /**
   * Initialize the repository. Base the implementation on the BackRestoreControllerImpl class methods.
   *
   * @return
   */
  private def initializeRepository: Task[Unit] =
    resticCommandService.init(repo, password = password).unit

  /**
   * Restore from the repository. Base the implementation on the BackRestoreControllerImpl class methods.
   *
   * @return
   */
  private def restoreFromRepository: Task[Unit] =
    val restoreOptions = RestoreOptions(
      exclude = config.restoreOptions.excludes,
      iexclude = config.restoreOptions.iexcludes,
      include = config.restoreOptions.includes,
      iinclude = config.restoreOptions.iincludes,
      tag = config.restoreOptions.tags,
      delete = config.restoreOptions.deleteExcluded,
      host = config.restoreOptions.hosts,
      target = Some(config.basePath)
    )

    for
      snapshots <- resticCommandService.snapshots(
        repo,
        password = password,
        snapshotsOptions = SnapshotsOptions(latest = 1)
      )
      anyBackupFolderExists <- ZIO.exists(config.backupFolders)(folder =>
        ZIO.attempt(Files.exists(config.basePath.resolve(folder).normalize))
      )
      _ <- snapshots.snapshots.headOption match
        case Some(snapshot) if config.restoreIfEmpty && !anyBackupFolderExists =>
          resticCommandService
            .restoreStream(
              repo,
              restoreOptions = restoreOptions,
              password = password,
              snapshotID = snapshot.id
            )
            .flatMap(_.runForeach {
              case status: RestoreMessage.Status =>
                commandQueue.offer(RestorePartial(status)).unit
              case summary: RestoreMessage.Summary =>
                history.update(_.addInitialRestoreSummary(summary))
              case error: RestoreMessage.Error =>
                ZIO.logWarning(
                  s"Restic reported a restore error during ${error.during} for ${error.item}: ${error.message}"
                )
              case _: RestoreMessage.VerboseStatus => ZIO.unit
            })
        case _ => ZIO.unit
    yield ()

  /**
   * Sets the state to WAITING_COMMAND.
   * Computes the next backup time, if any, runs a scheduler fiber that enqueues a ScheduledBackup command.
   */
  private def scheduleNextBackup: Task[Unit] =
    for
      now <- Clock.currentDateTime
      _ <- state.set(WAITING_COMMAND)
      _ <- config.backupCronSchedule.next(now) match
        case Some(nextRun) =>
          val delay =
            if nextRun.toInstant.isAfter(now.toInstant) then
              Duration.fromInterval(now.toInstant, nextRun.toInstant)
            else Duration.Zero
          Clock.sleep(delay) *> commandQueue.offer(ScheduledBackup).unit
        case None =>
          ZIO.logWarning("Backup cron schedule has no next execution time")
    yield ()

  /**
   * Executes a backup. Base the implementation on the BackRestoreControllerImpl class methods.
   */
  private def executeBackup: Task[Unit] =
    val backupOptions = BackupOptions(
      exclude = config.backupOptions.excludes,
      iexclude = config.backupOptions.iexcludes,
      tag = config.backupOptions.tags,
      oneFileSystem = config.backupOptions.oneFileSystem,
      readConcurrency = Option(config.backupOptions.readConcurrency).filter(_ > 0),
      skipIfChanged = config.backupOptions.skipIfChanged,
      host = Option(config.backupOptions.host).filterNot(_.isBlank)
    )
    ZIO.attemptBlockingIO(config.basePath.toFile.isDirectory).flatMap { isDir =>
      ZIO.unlessDiscard(isDir)(
        ZIO.fail(new IllegalStateException(
          s"Backup path is not a directory: ${config.basePath}"
        )))
    } *>
    resticCommandService
      .backupStream(
        repo = repo,
        backupOptions = backupOptions,
        password = password,
        basePath = config.basePath,
        paths = config.backupFolders
      )
      .flatMap(_.runForeach {
        case status: BackupMessage.Status =>
          commandQueue.offer(BackupPartial(status)).unit
        case summary: BackupMessage.Summary =>
          history.update(_.add(summary))
        case error: BackupMessage.Error =>
          ZIO.logWarning(
            s"Restic reported a backup error during ${error.during} for ${error.item}: ${error.message}"
          )
        case _: BackupMessage.VerboseStatus => ZIO.unit
      })

  private def stopController(cause : Cause[Any] = Cause.empty) = for {
    _ <- state.set(STOPPED(cause))
    cmds <- commandQueue.takeAll
    _ <- commandQueue.shutdown
    _ <- ZIO.foreach(cmds) {
      case BackupNow(reply) => reply.succeed(BackupRequestResult.Stopped)
      case _ => ZIO.unit
    }
  } yield ()

  private[BackupRestoreControllerImpl] def eventLoop: ZIO[Scope, Nothing, Unit] = {
    commandQueue.take.flatMap { cmd =>
      state.get.flatMap {
        case NOT_STARTED =>
          cmd match {
            case Start =>
              ZIO.logInfo("Starting backup/restore controller") *>
                state.set(STARTING) *>
                startController.catchAllCause(
                  insertError
                )
                *> eventLoop
            case _ =>
              ZIO.logWarning(s"Unexpected command in NOT_STARTED state: $cmd")
                *> eventLoop
          }
        case STARTING =>
          cmd match {
            case Initialize =>
              ZIO.logInfo("Initializing backup repository.") *>
                state.set(INITIALIZING)
                *> initializeRepository.foldCauseZIO(insertError, _ => commandQueue.offer(Schedule).unit).fork
                *> eventLoop
            case Restore =>
              ZIO.logInfo("Restoring from existing backup repository.")
                *> state.set(RESTORING())
                *> restoreFromRepository.foldCauseZIO(insertError,_ => commandQueue.offer(Schedule).unit).fork
                *> eventLoop
            case Stop =>
              ZIO.logInfo("Stopping backup respository start process.") *>
                stopController()
            case err@Error(cause) =>
              ZIO.logErrorCause(s"Error during backup/restore controller start", cause) *>
                stopController(cause)
            case Schedule =>
              ZIO.logInfo("Scheduling next backup.")
                *> scheduleNextBackup.catchAllCause(insertError).fork
                *> eventLoop
            case BackupNow(reply) =>
              ZIO.logWarning(s"Unexpected backup now command in STARTING state")
                *> reply.succeed(BackupRequestResult.NotReady)
                *> eventLoop
            case _ =>
              ZIO.logWarning(s"Unexpected command in STARTING state: $cmd")
              *> eventLoop
          }

        case RESTORING(_) =>
          cmd match {
            case Schedule =>
              ZIO.logInfo("Scheduling next backup.")
                *> scheduleNextBackup.catchAllCause(insertError).fork
                *> eventLoop
            case Stop =>
              ZIO.logInfo("Stopping restore process.")
                *> stopController()
            case RestorePartial(status) =>
              state.set(RESTORING(Some(status)))
              *> eventLoop
            case err@Error(cause) =>
              ZIO.logErrorCause(s"Error during restore process.", cause)
                *> stopController(cause)
            case BackupNow(reply) =>
              ZIO.logWarning(s"Unexpected backup now command in RESTORING state")
                *> reply.succeed(BackupRequestResult.NotReady)
                *> eventLoop
            case _ =>
              ZIO.logWarning(s"Unexpected command in RESTORING state: $cmd")
              *> eventLoop
          }
        case INITIALIZING => cmd match {
          case Schedule =>
            ZIO.logInfo("Scheduling next backup.")
              *> scheduleNextBackup.catchAllCause(insertError).fork
              *> eventLoop
          case Stop =>
            ZIO.logInfo("Stopping repository initialization process.")
              *> stopController()
          case err@Error(cause) =>
            ZIO.logErrorCause(s"Error during repository initialization process.", cause)
              *> stopController(cause)
          case BackupNow(reply) =>
            ZIO.logWarning(s"Unexpected backup now command in INITIALIZING state")
              *> reply.succeed(BackupRequestResult.NotReady)
              *> eventLoop
          case _ =>
            ZIO.logWarning(s"Unexpected command in INITIALIZING state: $cmd")
            *> eventLoop
        }
        case WAITING_COMMAND => cmd match {
          case BackupNow(reply) =>
            ZIO.logInfo("Processing backup now request")
              *> reply.succeed(BackupRequestResult.Accepted)
              *> state.set(BACKING_UP())
              *> executeBackup.foldCauseZIO(insertError,_ => commandQueue.offer(Schedule).unit).fork
              *> eventLoop
          case ScheduledBackup =>
            ZIO.logInfo("Processing scheduled backup")
              *> state.set(BACKING_UP())
              *> executeBackup.foldCauseZIO(insertError, _ => commandQueue.offer(Schedule).unit).fork
              *> eventLoop
          case Stop =>
            ZIO.logInfo("Stopping the controller.")
              *> stopController()
          case err@Error(cause) =>
            ZIO.logErrorCause(s"Error during waiting state.", cause)
              *> stopController(cause)
          case _ =>
            ZIO.logWarning(s"Unexpected command in WAITING_COMMAND state: $cmd")
            *> eventLoop
        }
        case BACKING_UP(_) =>
          cmd match {
            case Stop =>
              ZIO.logInfo("Stopping backup process.")
                *> stopController()
            case BackupPartial(status) =>
              state.set(BACKING_UP(Some(status)))
              *> eventLoop
            case Schedule =>
              ZIO.logInfo("Scheduling next backup.")
                *> scheduleNextBackup.catchAllCause(insertError).fork
                *> eventLoop
            case err@Error(cause) =>
              ZIO.logErrorCause(s"Error during backup process.", cause)
                *> stopController(cause)
            case BackupNow(reply) =>
              reply.succeed(BackupRequestResult.AlreadyRunning)
              *> eventLoop
            case _ =>
              ZIO.logWarning(s"Unexpected command in BACKING_UP state: $cmd")
              *> eventLoop
          }
        case STOPPED(_) =>
          ZIO.logWarning(s"Repository controller in stopped state. Ignoring command: $cmd")
      }
    }.unit
  }
  end eventLoop

  def currentHistory: UIO[BackupRestoreHistory] = history.get
  def currentState: UIO[State] = state.get
  def shutdown : UIO[Unit] =
    commandQueue.offer(Stop).unit *>
      commandFiberPromise.await.flatMap(_.join.ignore)

  def backupNow : UIO[BackupRequestResult] =
    for {
      p <- Promise.make[Nothing,BackupRequestResult]
      queued <- commandQueue.offer(BackupNow(p))
      _ <- ZIO.unless(queued) { p.succeed(BackupRequestResult.Stopped) }
      result <- p.await
    } yield result



end BackupRestoreControllerImpl

object BackupRestoreControllerImpl:


  enum Command:
    case Start
    case Initialize
    case Restore
    case RestorePartial(status: RestoreMessage.Status)
    case Schedule
    case BackupNow(reply: Promise[Nothing, BackupRequestResult])
    case ScheduledBackup
    case BackupPartial(status: BackupMessage.Status)
    case Stop
    case Error(cause: Cause[Any] = Cause.empty)

  def makeController(
    config : BackupRestoreControllerConfig,
    resticCommandService: ResticCommandService
  ): ZIO[Scope, Nothing, BackupRestoreController] =
    for {
      _ <- ZIO.logInfo(s"Initializing backup/restore controller for ${config.resticRepo.repoPath}")
      commandQueue <- Queue.bounded[Command](100)
      state <- SubscriptionRef.make[State](State.NOT_STARTED)
      history <- SubscriptionRef.make[BackupRestoreHistory](BackupRestoreHistory(backupSummariesCapacity = config.historyCapacity))
      commandFiberPromise <- Promise.make[Nothing, Fiber[Throwable, Unit]]
      controller = BackupRestoreControllerImpl(config, commandQueue, state, history, commandFiberPromise, resticCommandService)
      commandFiber <- controller.eventLoop.forkScoped
      _ <- commandFiberPromise.succeed(commandFiber)
      _ <- commandQueue.offer(Command.Start)
    } yield controller

  val controllersLayer: ZLayer[ResticCommandService, Config.Error, BackupRestoreControllers] = ZLayer.scoped {
    for {
      configs <- ZIO.config(BackupRestoreControllerConfig.configs)
      resticCommandService <- ZIO.service[ResticCommandService]
      controllers <- ZIO.foreach(configs) { config =>
        ZIO.acquireRelease(BackupRestoreControllerImpl.makeController(config, resticCommandService))(_.shutdown)
      }
    } yield new BackupRestoreControllers {
      val controllers: Chunk[BackupRestoreController] = controllers.to(Chunk)
    }
  }