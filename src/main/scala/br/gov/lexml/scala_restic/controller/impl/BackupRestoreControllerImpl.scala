package br.gov.lexml.scala_restic.controller.impl

import zio.*
import zio.stream.*
import br.gov.lexml.scala_restic.command.ResticCommandService
import br.gov.lexml.scala_restic.controller.impl.BackupRestoreControllerImpl.{BackupRestoreHistory, Command, State}
import br.gov.lexml.scala_restic.data.backup.BackupMessage
import br.gov.lexml.scala_restic.data.restore.RestoreMessage
import br.gov.lexml.scala_restic.options.backup.BackupOptions
import br.gov.lexml.scala_restic.options.common.Repo
import br.gov.lexml.scala_restic.options.restore.RestoreOptions
import br.gov.lexml.scala_restic.options.snapshots.SnapshotsOptions
import cron4s.*
import cron4s.lib.javatime.*

import java.nio.file.{Files, Path}

class BackupRestoreControllerImpl(
  config: BackupRestoreControllerConfig,
  commandQueue: Queue[BackupRestoreControllerImpl.Command],
  state: SubscriptionRef[BackupRestoreControllerImpl.State],
  history: SubscriptionRef[BackupRestoreControllerImpl.BackupRestoreHistory],
  commandFiberPromise: Promise[Nothing, Fiber[Throwable, Unit]],
  resticCommandService: ResticCommandService,
):

  import BackupRestoreControllerImpl.*
  import State.*
  import Command.*

  private val repo = Repo.atFolder(config.resticRepo.repoPath)
  private val password = Option(config.resticRepo.repoPassword).filterNot(_.isBlank)

  private def insertError(cause: Cause[Throwable]) =
    commandQueue.takeAll.flatMap { chunk => commandQueue.offer(Error(cause)) *> commandQueue.offerAll(chunk) }

  private def insertStop =
    commandQueue.takeAll.flatMap { chunk => commandQueue.offer(Stop) }

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
      target = Some(Path.of("/"))
    )

    for
      snapshots <- resticCommandService.snapshots(
        repo,
        password = password,
        snapshotsOptions = SnapshotsOptions(latest = 1)
      )
      anyBackupFolderExists <- ZIO.exists(config.backupFolders)(folder =>
        ZIO.attempt(Files.exists(folder))
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
                history.update(_.copy(initialRestoreSummary = Some(summary)))
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

    resticCommandService
      .backupStream(
        repo = repo,
        backupOptions = backupOptions,
        password = password,
        paths = config.backupFolders
      )
      .flatMap(_.runForeach {
        case status: BackupMessage.Status =>
          commandQueue.offer(BackupPartial(status)).unit
        case summary: BackupMessage.Summary =>
          history.update(current =>
            current.copy(backupSummaries = current.backupSummaries :+ summary)
          )
        case error: BackupMessage.Error =>
          ZIO.logWarning(
            s"Restic reported a backup error during ${error.during} for ${error.item}: ${error.message}"
          )
        case _: BackupMessage.VerboseStatus => ZIO.unit
      })

  def start: ZIO[Scope, Nothing, Unit] = for {
    _ <- state.set(State.NOT_STARTED)
    _ <- commandQueue.offer(Start)
    commandFiber <- eventLoop.forkScoped
    _ <- commandFiberPromise.succeed(commandFiber)
  } yield ()

  private def eventLoop: ZIO[Scope, Nothing, Unit] =
    commandQueue.take.flatMap { cmd =>
      state.get.flatMap {
        case NOT_STARTED =>
          cmd match {
            case Start =>
              ZIO.logInfo("Starting backup/restore controller") *>
                state.set(STARTING) *>
                startController.catchAllCause(
                  insertError
                ).forkScoped
            case _ =>
              ZIO.logWarning(s"Unexpected command in NOT_STARTED state: $cmd")
          }
        case STARTING =>
          cmd match {
            case Start =>
              ZIO.logInfo("Starting backup/restore controller") *>
                startController.catchAllCause(insertError).forkScoped
            case Initialize =>
              ZIO.logInfo("Initializing backup repository.") *>
                state.set(INITIALIZING) *>
                (initializeRepository.catchAllCause(insertError) *> commandQueue.offer(Schedule)).forkScoped
            case Restore =>
              ZIO.logInfo("Restoring from existing backup repository.") *>
                state.set(RESTORING()) *>
                (restoreFromRepository.catchAllCause(insertError) *> commandQueue.offer(Schedule)).forkScoped
            case Stop =>
              ZIO.logInfo("Stopping backup respository start process.") *>
                state.set(STOPPED())
            case err@Error(cause) =>
              ZIO.logErrorCause(s"Error during backup/restore controller start", cause) *>
                state.set(STOPPED(cause))
            case Schedule =>
              ZIO.logInfo("Scheduling next backup.") *>
                scheduleNextBackup.catchAllCause(insertError).forkScoped
            case BackupNow(reply) =>
              ZIO.logWarning(s"Unexpected backup now command in STARTING state") *>
                reply.succeed(BackupRequestResult.NotReady)
            case _ =>
              ZIO.logWarning(s"Unexpected command in STARTING state: $cmd")
          }

        case RESTORING(_) =>
          cmd match {
            case Schedule =>
              ZIO.logInfo("Scheduling next backup.") *>
                scheduleNextBackup.catchAllCause(insertError).forkScoped
            case Stop =>
              ZIO.logInfo("Stopping restore process.") *>
                state.set(STOPPED())
            case RestorePartial(status) =>
              state.set(RESTORING(Some(status)))
            case err@Error(cause) =>
              ZIO.logErrorCause(s"Error during restore process.", cause) *>
                state.set(STOPPED(cause))
            case BackupNow(reply) =>
              ZIO.logWarning(s"Unexpected backup now command in RESTORING state") *>
                reply.succeed(BackupRequestResult.NotReady)
            case _ =>
              ZIO.logWarning(s"Unexpected command in RESTORING state: $cmd")
          }
        case INITIALIZING => cmd match {
          case Schedule =>
            ZIO.logInfo("Scheduling next backup.") *>
              scheduleNextBackup.catchAllCause(insertError).forkScoped
          case Stop =>
            ZIO.logInfo("Stopping repository initialization process.") *>
              state.set(STOPPED())
          case err@Error(cause) =>
            ZIO.logErrorCause(s"Error during repository initialization process.", cause) *>
              state.set(STOPPED(cause))
          case BackupNow(reply) =>
            ZIO.logWarning(s"Unexpected backup now command in INITIALIZING state") *>
              reply.succeed(BackupRequestResult.NotReady)
          case _ =>
            ZIO.logWarning(s"Unexpected command in INITIALIZING state: $cmd")
        }
        case WAITING_COMMAND => cmd match {
          case BackupNow(reply) =>
            ZIO.logInfo("Processing backup now request") *>
              reply.succeed(BackupRequestResult.Accepted) *>
              state.set(BACKING_UP()) *>
              (executeBackup.catchAllCause(insertError) *> commandQueue.offer(Schedule)).forkScoped
          case ScheduledBackup =>
            ZIO.logInfo("Processing scheduled backup") *>
              state.set(BACKING_UP()) *>
              (executeBackup.catchAllCause(insertError) *> commandQueue.offer(Schedule)).forkScoped
          case Stop =>
            ZIO.logInfo("Stopping repository initialization process.") *>
              state.set(STOPPED())
          case err@Error(cause) =>
            ZIO.logErrorCause(s"Error during waiting state.", cause) *>
              state.set(STOPPED(cause))
          case _ =>
            ZIO.logWarning(s"Unexpected command in WAITING_COMMAND state: $cmd")
        }
        case BACKING_UP(_) =>
          cmd match {
            case Stop =>
              ZIO.logInfo("Stopping backup process.") *>
                state.set(STOPPED())
            case BackupPartial(status) =>
              state.set(BACKING_UP(Some(status)))
            case Schedule =>
              ZIO.logInfo("Scheduling next backup.") *>
                scheduleNextBackup.catchAllCause(insertError).forkScoped
            case err@Error(cause) =>
              ZIO.logErrorCause(s"Error during restore process.", cause) *>
                state.set(STOPPED(cause))
            case BackupNow(reply) =>
              reply.succeed(BackupRequestResult.AlreadyRunning)
            case _ =>
              ZIO.logWarning(s"Unexpected command in BACKING_UP state: $cmd")
          }
        case STOPPED(_) =>
          ZIO.logWarning(s"Repository controller in stopped state. Ignoring command: $cmd")
      }
    }.unit.forever
  end eventLoop

  def currentHistory: UIO[BackupRestoreHistory] = history.get
  def currentState: UIO[State] = state.get
  def shutdown : UIO[Unit] =
    insertStop *>
      ZIO.sleep(5.seconds) *>
      commandFiberPromise.await.flatMap(_.interrupt).unit

end BackupRestoreControllerImpl

object BackupRestoreControllerImpl:
  final case class BackupRestoreHistory(
    initialRestoreSummary: Option[RestoreMessage.Summary] = None,
    backupSummaries: List[BackupMessage.Summary] = List.empty
  )

  enum State:
    case NOT_STARTED
    case STARTING
    case INITIALIZING
    case RESTORING(lastStatus: Option[RestoreMessage.Status] = None)
    case WAITING_COMMAND
    case BACKING_UP(lastStatus: Option[BackupMessage.Status] = None)
    case STOPPED(cause: Cause[Any] = Cause.empty)

  enum BackupRequestResult:
    case NotReady, Accepted, AlreadyRunning, Stopped

  enum Command:
    case Start
    case FinishStart
    case Initialize
    case Restore
    case RestorePartial(status: RestoreMessage.Status)
    case Schedule
    case BackupNow(reply: Promise[Nothing, BackupRequestResult])
    case ScheduledBackup
    case BackupPartial(status: BackupMessage.Status)
    case Stop
    case Error(cause: Cause[Any] = Cause.empty)
end BackupRestoreControllerImpl

final case class BackupRestoreControllers(
  controllers : Chunk[BackupRestoreControllerImpl]
)

object BackupRestoreControllers:
  val layer: ZLayer[Scope & ResticCommandService, Config.Error, BackupRestoreControllers] = ZLayer.fromZIO {
    for {
      configs <- ZIO.config(BackupRestoreControllerConfig.configs)
      controllers <- ZIO.foreach(configs) { config =>
        for {
          _ <- ZIO.logInfo(s"Initializing backup/restore controller for ${config.resticRepo.repoPath}")
          commandQueue <- Queue.bounded[Command](100)
          state <- SubscriptionRef.make[State](State.NOT_STARTED)
          history <- SubscriptionRef.make[BackupRestoreHistory](BackupRestoreHistory())
          commandFiberPromise <- Promise.make[Nothing, Fiber[Throwable, Unit]]
          resticCommandService <- ZIO.service[ResticCommandService]
          controller = BackupRestoreControllerImpl(config, commandQueue, state, history, commandFiberPromise, resticCommandService)
          commandFiber <- controller.start.forkScoped
          _ <- commandFiberPromise.succeed(commandFiber)
        } yield controller
      }
    } yield BackupRestoreControllers(controllers.to(Chunk))
  }