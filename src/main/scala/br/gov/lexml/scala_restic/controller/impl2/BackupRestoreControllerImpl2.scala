package br.gov.lexml.scala_restic.controller.impl2

import zio.*
import zio.stream.*
import zio.config.*
import zio.config.magnolia.*
import BackupRestoreControllerImpl2.{Message, State}
import br.gov.lexml.scala_restic.command.ResticCommandService
import br.gov.lexml.scala_restic.controller.BackupRestoreController.State.WAITING_COMMAND
import br.gov.lexml.scala_restic.controller.impl.BackupRestoreControllerConfig
import br.gov.lexml.scala_restic.data.restore.RestoreMessage
import br.gov.lexml.scala_restic.options.common.Repo
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
  private val eventLoopFiberPromise : Promise[Nothing,Fiber[Nothing,Unit]],
  private val stateRef : SubscriptionRef[State],
  private val backupScheduleFiberRef : Ref[Option[Fiber[Nothing,Unit]]],
  private val resticCommandService : ResticCommandService
):

  private val repo = Repo.atFolder(config.resticRepo.repoPath)
  private val password = Option(config.resticRepo.repoPassword).filterNot(_.isBlank)

  private def start = for {
    eventLoopFiber <- eventLoop.forkScoped
    _ <- eventLoopFiberPromise.succeed(eventLoopFiber)
    _ <- queue.offer(Message.Start)
  } yield ()

  import State.*
  import Message.*

  final case class ProcessStep(task : Task[Option[(State,Option[ProcessStep])]])

  private def eventLoop : UIO[Unit] = {
    def executeProcess(process : ProcessStep) : UIO[Unit] =
      process.task.foldCauseZIO(
        cause => stateRef.set(Stopped(cause)),
        {
          case None => stateRef.set(Stopped())
          case Some((nextState2,optProcess)) =>
            stateRef.set(nextState2) *>
            optProcess.fold(loop)(executeProcess)
        })
    def loop : UIO[Unit] = for {
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

  private def step(state : State, task : StepTask) =
    ZIO.succeed(Some((state,Some(ProcessStep(task)))))

  private def step(state: State) =
    ZIO.succeed(Some((state, None)))
    
  private def stepOut = ZIO.succeed(None)  

  private val toScheduleStep : StepTask = step(Starting,scheduleTask)

  private val toRestoreStep : StepTask =
    ZIO.clockWith(_.currentDateTime).flatMap { now => step(Restoring(now),restoreTask) }

  private def fail(message : String) =  ZIO.fail(Exception(message))

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
          case Some(snapshot) if config.restoreIfEmpty && !anyBackupFolderExists => toRestoreStep
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

  private def restoreTask : StepTask = ???

  


object BackupRestoreControllerImpl2:
  private enum Message:
    case Start
    case ScheduledBackup

  enum State:
    case Stopped(cause: Cause[Any] = Cause.empty) extends State
    case Starting
    case Ready
    case Restoring(startTime : OffsetDateTime, latestSummary: Option[RestoreMessage.Summary] = None) extends State
