package br.gov.lexml.scala_restic.controller

import br.gov.lexml.scala_restic.data.backup.BackupMessage
import br.gov.lexml.scala_restic.data.init.InitResult
import br.gov.lexml.scala_restic.data.restore.RestoreMessage
import br.gov.lexml.scala_restic.data.snapshots.Snapshot
import br.gov.lexml.scala_restic.options.backup.BackupOptions
import br.gov.lexml.scala_restic.options.common.CommonOptions
import br.gov.lexml.scala_restic.options.restore.RestoreOptions
import br.gov.lexml.scala_restic.options.snapshots.SnapshotsOptions
import zio.*
import zio.stream.*

import java.nio.file.Path

abstract sealed class RepoControllerException(val message : String, val cause : Cause[Throwable] = Cause.empty)
  extends Exception(s"$message: $cause.prettyPrint")

final case class GeneralRepoControllerException(override val message : String, override val cause : Cause[Throwable] = Cause.empty)
  extends RepoControllerException(message,cause)


/*
Restore
 */
final case class RepoRestoreException(override val message : String, override val cause : Cause[Throwable] = Cause.empty)
  extends RepoControllerException(message,cause)
final case class RepoRestoreItemException(errorMessage : RestoreMessage.Error)
  extends RepoControllerException(s"Error during restore: message=${errorMessage.message}, during=${errorMessage.during}, item=${errorMessage.item}",Cause.empty)

trait ProcessData[Summary,Status]:
  /*
    This stream is live-only: it will not emit any element until the process is started and may miss elements if
    the method is called after the process has already started.
     */
  def statusStream: ZStream[Any, Nothing, Status]

  def awaitSummary: IO[RepoControllerException, Summary]

  def cancel: UIO[Unit]

type RepoRestoreData = ProcessData[RestoreMessage.Summary,RestoreMessage.Status]


/*
Backup
 */
final case class RepoBackupException(override val message : String, override val cause : Cause[Throwable] = Cause.empty)
  extends RepoControllerException(message,cause)
final case class RepoBackupItemException(errorMessage : BackupMessage.Error)
  extends RepoControllerException(s"Error during backup: message=${errorMessage.message}, during=${errorMessage.during}, item=${errorMessage.item}",Cause.empty)

type RepoBackupData = ProcessData[BackupMessage.Summary,BackupMessage.Status]


trait ResticRepoController:
  def repoExists(commonOptions: CommonOptions = CommonOptions()) : Task[Boolean]
  def init(commonOptions: CommonOptions = CommonOptions()) : Task[InitResult]
  def snapshots(
    commonOptions: CommonOptions = CommonOptions(),
    snapshotOptions : SnapshotsOptions = SnapshotsOptions()) : Task[Vector[Snapshot]]
  def restore(snapshotID : String = "latest", commonOptions : CommonOptions = CommonOptions(), restoreOptions : RestoreOptions = RestoreOptions()) :
      ZIO[Scope,RepoControllerException,RepoRestoreData]
  def backup(commonOptions : CommonOptions = CommonOptions(), backupOptions : BackupOptions = BackupOptions()) :
      ZIO[Scope,RepoControllerException,RepoBackupData]

