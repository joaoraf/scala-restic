package br.gov.lexml.scala_restic.controller

import br.gov.lexml.scala_restic.data.backup.BackupMessage
import br.gov.lexml.scala_restic.data.restore.RestoreMessage
import zio.*
import zio.stream.*

import scala.collection.immutable.Queue as SQueue

trait BackupRestoreController:
  def currentHistory: UIO[BackupRestoreController.BackupRestoreHistory]
  def currentState: UIO[BackupRestoreController.State]
  def backupNow : UIO[BackupRestoreController.BackupRequestResult]
  def shutdown : UIO[Unit]

object BackupRestoreController:
  enum State:
    case NOT_STARTED
    case STARTING
    case INITIALIZING
    case RESTORING(lastStatus: Option[RestoreMessage.Status] = None)
    case WAITING_COMMAND
    case BACKING_UP(lastStatus: Option[BackupMessage.Status] = None)
    case STOPPED(cause: Cause[Any] = Cause.empty)

  final case class BackupRestoreHistory(
    initialRestoreSummary: Option[RestoreMessage.Summary] = None,
    backupSummaries: SQueue[BackupMessage.Summary] = SQueue.empty,
    backupSummariesCapacity: Int
  ):
    require(backupSummariesCapacity > 0)
    def addInitialRestoreSummary(summary: RestoreMessage.Summary): BackupRestoreHistory =
      copy(initialRestoreSummary = Some(summary))

    def add(summary: BackupMessage.Summary): BackupRestoreHistory =
      if backupSummaries.size >= backupSummariesCapacity then
        copy(
          backupSummaries = backupSummaries.dequeue._2.enqueue(summary)
        )
      else
        copy(backupSummaries = backupSummaries.enqueue(summary))

  enum BackupRequestResult:
    case NotReady, Accepted, AlreadyRunning, Stopped

trait BackupRestoreControllers:
  def controllers : Chunk[BackupRestoreController]
