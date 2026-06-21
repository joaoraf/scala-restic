package br.gov.lexml.scala_restic.controller.impl

import zio.*
import zio.stream.*
import br.gov.lexml.scala_restic.data.backup.BackupMessage
import br.gov.lexml.scala_restic.data.restore.RestoreMessage


object BackupRestoreControllerImpl2:
  final case class BackupRestoreHistory(
    initialRestoreSummary : Option[RestoreMessage.Summary] = None,
    backupSummaries : List[BackupMessage.Summary] = List.empty
  )
  enum State:
    case NOT_STARTED
    case STARTING
    case INITIALIZING
    case RESTORING(lastStatus : Option[RestoreMessage.Status] = None)
    case WAITING_COMMAND
    case BACKING_UP(lastStatus : Option[BackupMessage.Status] = None)
    case STOPPED(cause : Cause[Any] = Cause.empty)
  enum BackupRequestResult:
    case NotReady, Accepted, AlreadyRunning, Stopped
  enum Command:
    case Start
    case FinishStart
    case Initialize
    case Restore
    case RestorePartial(status : RestoreMessage.Status)
    case Schedule
    case BackupNow(reply : Promise[Nothing, BackupRequestResult])
    case ScheduledBackup
    case BackupPartial(status : BackupMessage.Status)
    case Stop
    case Error(cause : Cause[Any] = Cause.empty)


class BackupRestoreControllerImpl2(
  config : BackupRestoreControllerConfig,
  commandQueue : Queue[BackupRestoreControllerImpl2.Command],
  state : SubscriptionRef[BackupRestoreControllerImpl2.State],
  history : SubscriptionRef[BackupRestoreControllerImpl2.BackupRestoreHistory],
  commandFiberPromise : Promise[Nothing,Fiber[Throwable, Unit]],
):
  import BackupRestoreControllerImpl2.*
  import State.*
  import Command.*

  private def insertError(cause : Cause[Throwable]) =
    commandQueue.takeAll.flatMap { chunk => commandQueue.offer(Error(cause)) *> commandQueue.offerAll(chunk) }

  /**
   * Initialize the controller. Checks the existence of the repository.
   * If the repository does not exist and the configuration allows initialization,
   * enqueue the Initialize command, otherwise, enqueue an error command.
   * If the repository exists, enqueue the restore command.
   */
  private def startController : Task[Unit] = ???

  /**
   * Initialize the repository. Base the implementation on the BackRestoreControllerImpl class methods.
   * @return
   */
  private def initializeRepository : Task[Unit] = ???

  /**
   * Restore from the repository. Base the implementation on the BackRestoreControllerImpl class methods.
   * @return
   */
  private def restoreFromRepository : Task[Unit] = ???

  /**
   * Sets the state to WAITING_COMMAND.
   * Computes the next backup time, if any, runs a scheduler fiber that enqueues a ScheduledBackup command.
   */
  private def scheduleNextBackup : Task[Unit] = ???
  /**
   * Executes a backup. Base the implementation on the BackRestoreControllerImpl class methods.
   */
  private def executeBackup : Task[Unit] = ???

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
                (initializeRepository.catchAllCause(insertError) *> scheduleNextBackup).forkScoped
            case Restore =>
              ZIO.logInfo("Restoring from existing backup repository.") *>
                state.set(RESTORING()) *>
                (restoreFromRepository.catchAllCause(insertError) *> scheduleNextBackup).forkScoped
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
              executeBackup.catchAllCause(insertError).forkScoped
          case ScheduledBackup =>
            ZIO.logInfo("Processing scheduled backup") *>
              state.set(BACKING_UP()) *>
              (executeBackup.catchAllCause(insertError) *> scheduleNextBackup).forkScoped
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



