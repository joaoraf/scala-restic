package br.gov.lexml.scala_restic.controller.impl2

import zio.*
import zio.stream.*
import zio.config.*
import zio.config.magnolia.*
import BackupRestoreControllerImpl2.{Message, ResticRestoreException, State}
import br.gov.lexml.scala_restic.command.ResticCommandService
import br.gov.lexml.scala_restic.controller.BackupRestoreController.State.WAITING_COMMAND
import br.gov.lexml.scala_restic.controller.impl.BackupRestoreControllerConfig
import br.gov.lexml.scala_restic.data.restore.RestoreMessage
import br.gov.lexml.scala_restic.options.common.Repo
import br.gov.lexml.scala_restic.options.restore.RestoreOptions
import br.gov.lexml.scala_restic.options.snapshots.SnapshotsOptions
import cron4s.*
import cron4s.lib.javatime.*

import java.nio.file.Files
import java.time.OffsetDateTime


enum BackupRestoreControllerImplMessage:
  case BackupNowAsync(reply : Promise[Throwable,Unit])
  case BackupNowSync(reply : Promise[Throwable,Unit])

class BackupRestoreControllerImpl2 private (
  private val config: BackupRestoreControllerConfig,,
  private val queue : Queue[BackupRestoreControllerImpl2.Message],
  private val eventLoopFiberPromise : Promise[Nothing,Fiber[Throwable,Unit]],
  private val stateRef : SubscriptionRef[State],
  private val backupScheduleFiberRef : Ref[Option[Fiber[Nothing,Unit]]],
  private val childFiberFailureCause : Ref[Cause[Throwable]],
  private val resticCommandService : ResticCommandService
):
  import State.*
  import Message.*

  private val repo = Repo.atFolder(config.resticRepo.repoPath)
  private val password = Option(config.resticRepo.repoPassword).filterNot(_.isBlank)

  extension [Env,E <: Throwable,A] (effect : ZIO[Env,E,A])
    def forkCatchAndSignalError: URIO[Env, Fiber.Runtime[E, A]] =
      effect.catchAllCause { cause =>
        childFiberFailureCause.update(c => c ++ cause)
        eventLoopFiberPromise.poll.flatMap {
          case Some(f) => f.flatMap(_.interrupt)
          case None => ZIO.logWarning(s"Error caught in child fiber, but the eventLoopFiberPromise is undefined: $cause !")
        }
        ZIO.refailCause(cause)
      }.fork

  private def monitorEventLoop = for {
    eventLoopFiber <- eventLoop.fork
    _ <- childFiberFailureCause.set(Cause.empty)
    _ <- eventLoopFiber.await.flatMap {
      case Exit.Success =>
        stateRef.set(State.Stopped(Cause.empty))
      case Exit.Failure(cause) =>
        val cause1 = if cause.isInterruptedOnly then Cause.empty else cause
        childFiberFailureCause.getAndSet(Cause.empty).flatMap {
          c => stateRef.set(State.Stopped(cause1 ++ c))
        }
    }
  } yield eventLoopFiber

  private def start = for {
    eventLoopFiber <- eventLoop.forkScoped
    _ <- eventLoopFiberPromise.succeed(eventLoopFiber)
    _ <- queue.offer(Message.Start)
  } yield ()

  private final case class ProcessStep(task : Task[Option[(State,Option[ProcessStep])]])

  private def eventLoop : Task[Unit] = {
    def executeProcess(process : ProcessStep) : Task[Unit] =
      process.task.flatMap {
          case None => ZIO.unit
          case Some((nextState2,optProcess)) =>
            stateRef.set(nextState2) *>
            optProcess.fold(loop)(executeProcess)
        }
    def loop : Task[Unit] = for {
      currentState <- stateRef.get
      nextMessage <- queue.take
      process = processStateMessage(currentState,nextMessage)
      _ <- executeProcess(process)
    } yield ()
    loop
  }

  private def processStateMessage(currentState: State, message: Message): ProcessStep =
    (currentState, message) match {
      case (Stopped(_), Start) => ProcessStep(startProcess)
    }

  private type StepTask = Task[Option[(State,Option[ProcessStep])]]

  private inline def step(state : State, task : StepTask) =
    ZIO.succeed(Some((state,Some(ProcessStep(task)))))

  private inline def step(state: State) =
    ZIO.succeed(Some((state, None)))
    
  private inline def stepOut = ZIO.succeed(None)

  private inline val toScheduleStep : StepTask = step(Starting,scheduleTask)

  private inline def toRestoreStep(snapshotID : String) : StepTask =
    ZIO.clockWith(_.currentDateTime).flatMap { now => step(Restoring(now),restoreTask(snapshotID)) }

  private inline def fail(message : String) =  ZIO.fail(Exception(message))

  private def startProcess : StepTask =
    // Check if the restic repo path exists.
    def checkRepositoryExistence : StepTask =
      ZIO.ifZIO(ZIO.attemptBlockingIO(Files.exists(config.resticRepo.repoPath)))(whenPathExists, whenPathDoesNotExist)
    // If not, create it if the initIfNecessary flag is set, otherwise throw an exception.
    def whenPathDoesNotExist : StepTask =
      if(config.initIfNecessary) then
        resticCommandService.init(repo,password = password) *> toScheduleStep
      else
        fail("Restic backup folder does not exists and initialization is not enabled.")

    // If the repo path exists, check if it's a valid restic repo using the cat info command.
    // If not, throw an exception.
    def whenPathExists : StepTask =
      ZIO.ifZIO(resticCommandService.checkRepositoryExistence(repo,password = password))(
        checkIfShouldRestore
        , fail("Restic backup folder exists but is not a valid restic repository.")
      )
    def checkIfShouldRestore : StepTask =
      for {
        snapshots <- resticCommandService.snapshots(
          repo,
          password = password,
          snapshotsOptions = SnapshotsOptions(latest = 1)
        )
        anyBackupFolderExists <- ZIO.exists(config.backupFolders)(folder =>
          ZIO.attempt(Files.exists(config.basePath.resolve(folder).normalize))
        )
        result <- snapshots.snapshots.headOption match {
          case Some(snapshot) if config.restoreIfEmpty && !anyBackupFolderExists => toRestoreStep(snapshot.id)
          case _ => toScheduleStep
        }
      } yield result
    checkRepositoryExistence
  end startProcess

  private def scheduleTask : StepTask =
    Clock.currentDateTime.flatMap { now =>
      config.backupCronSchedule.next(now) match
        case Some(nextRun) =>
          val delay =
            if nextRun.toInstant.isAfter(now.toInstant) then
              Duration.fromInterval(now.toInstant, nextRun.toInstant)
            else Duration.Zero
          for {
            _ <- backupScheduleFiberRef.get.flatMap {
              case Some(fiber) => fiber.interrupt
              case None => ZIO.unit
            }
            newBackupScheduleFiber <- (Clock.sleep(delay) *> queue.offer(ScheduledBackup)).unit.fork
            _ <- backupScheduleFiberRef.set(Some(newBackupScheduleFiber))
          } yield ()
        case None => ZIO.unit
    }.map(_ => Some((Ready,None)))

  private def restoreTask(snapshotID : String) : StepTask =
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
    for {
      now <- ZIO.clockWith(_.currentDateTime)
      stream <- resticCommandService.restoreStream(
        repo,
        restoreOptions = restoreOptions,
        password = password,
        snapshotID = snapshotID
      )
      _ <- stream.foreach {
        case status: RestoreMessage.Status => queue.offer(Message.PartialRestoreStatus(status))
        case summary: RestoreMessage.Summary => queue.offer(Message.RestoreCompleted(summary))
        case error: RestoreMessage.Error => ZIO.fail(ResticRestoreException(error))
        case _: RestoreMessage.VerboseStatus => ZIO.unit
      }.forkCatchAndSignalError
    } yield Some((Restoring(now),None))
  end restoreTask

object BackupRestoreControllerImpl2:
  final case class ResticRestoreException(error : RestoreMessage.Error)
    extends Exception(s"Error during restic restore: ${error.message} on ${error.item} during ${error.during}")
  private enum Message:
    case Start
    case ScheduledBackup
    case PartialRestoreStatus(status : RestoreMessage.Status)
    case RestoreCompleted(summary : RestoreMessage.Summary)

  enum State:
    case Stopped(cause: Cause[Any] = Cause.empty) extends State
    case Starting
    case Ready
    case Restoring(startTime : OffsetDateTime, latestSummary: Option[RestoreMessage.Summary] = None) extends State
