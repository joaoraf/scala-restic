package br.gov.lexml.scala_restic.controller.impl2

import zio.*
import zio.stream.*
import zio.config.*
import zio.config.magnolia.*
import BackupRestoreControllerImpl2.{Message, State}
import br.gov.lexml.scala_restic.command.ResticCommandService

final case class BackupRestoreControllerImplConfig(
)

enum BackupRestoreControllerImplMessage:
  case BackupNowAsync(reply : Promise[Throwable,Unit])
  case BackupNowSync(reply : Promise[Throwable,Unit])

class BackupRestoreControllerImpl2 private (
  private val config : BackupRestoreControllerImplConfig,
  private val queue : Queue[BackupRestoreControllerImpl2.Message],
  private val eventLoopFiberPromise : Promise[Nothing,Fiber[Nothing,Unit]],
  private val stateRef : SubscriptionRef[State],
  private val resticCommandService : ResticCommandService
):
  private def start = for {
    eventLoopFiber <- eventLoop.forkScoped
    _ <- eventLoopFiberPromise.succeed(eventLoopFiber)
    _ <- queue.offer(Message.Start)
  } yield ()

  import State.*
  import Message.*

  final case class Process(task : Task[Option[(State,Option[Process])]])

  private def eventLoop : UIO[Unit] = {
    def executeProcess(process : Process) : UIO[Unit] =
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

  private def startProcess : Task[Option[(State,Option[Process])]] =
      // Check if the restic repo path exists.
      // If not, create it if the initIfNecessary flag is set, otherwise throw an exception.
      // If the repo path exists, check if it's a valid restic repo using the cat info command.
      // If not, throw an exception.
      // If the repo path exists and is a valid restic repo and the initialRestore is set, return the
      // Restoring state and the restore process.
      // Otherwise, call the schedule task.
    ZIO.ifZIO(ZIO.attemptBlockingIO(config.repo.repoPath.toFile.exists))(
      onFalse =
        if config.initIfNecessary then
          resticCommandService.init(repo,password = config.password))(
            ZIO.unit
            ,
            ZIO.unit
          )
        else ZIO.fail(new Exception("Repository does not exist!")),
      onTrue =
        if config.restoreOnStart then
          ZIO.succeed()
    )

  private def processStateMessage(currentState : State, message : Message) : Process =
    (currentState,message) match {
      case (Stopped(_),Start) => Process(startProcess)
    }

  private def doStart : Task[Option[State]] = ???

object BackupRestoreControllerImpl2:
  private enum Message:
    case Start

  enum State:
    case Stopped(cause: Cause[Any] = Cause.empty) extends State
    case Starting
