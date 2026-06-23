package br.gov.lexml.scala_restic.controller.impl2

import zio.*
import zio.stream.*
import zio.config.*
import zio.config.magnolia.*
import BackupRestoreControllerImpl2.{Message,State}

final case class BackupRestoreControllerImplConfig(
)

enum BackupRestoreControllerImplMessage:
  case BackupNowAsync(reply : Promise[Throwable,Unit])
  case BackupNowSync(reply : Promise[Throwable,Unit])

class BackupRestoreControllerImpl2 private (
  private val config : BackupRestoreControllerImplConfig,
  private val queue : Queue[BackupRestoreControllerImpl2.Message],
  private val eventLoopFiberPromise : Promise[Nothing,Fiber[Nothing,Unit]],
  private val stateRef : SubscriptionRef[State]
):
  private def start = for {
    eventLoopFiber <- eventLoop.forkScoped
    _ <- eventLoopFiberPromise.succeed(eventLoopFiber)
    _ <- queue.offer(Message.Start)
  } yield ()

  import State.*
  import Message.*

  private def eventLoop : UIO[Unit] = {
    def loop : UIO[Unit] = for {
      currentState <- stateRef.get
      nextMessage <- queue.take
      nextStateAndProcess <- processStateMessage(currentState,nextMessage)
      (nextState,process) = nextStateAndProcess
      _ <- stateRef.set(nextState)
      _ <- process.foldCauseZIO(
        cause => stateRef.set(Stopped(cause)),
        {
          case None => stateRef.set(Stopped())
          case Some(nextState2) => stateRef.set(nextState2) *> loop
        }
      )
    } yield ()
    loop
  }

  private def processStateMessage(currentState : State, message : Message)
  : UIO[(State,Task[Option[State]])] =
    (currentState,message) match {
      case (Stopped(_),Start) =>
        ZIO.succeed((Starting,doStart))
    }

  private def doStart : Task[Option[State]] = ???

object BackupRestoreControllerImpl2:
  private enum Message:
    case Start

  enum State:
    case Stopped(cause: Cause[Any] = Cause.empty) extends State
    case Starting
