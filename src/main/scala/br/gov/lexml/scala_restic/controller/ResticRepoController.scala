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
trait RepoRestoreData:
  def statusHub : Hub[RestoreMessage.Status]
  def awaitSummary : IO[RepoControllerException,RestoreMessage.Summary]
  def cancel : UIO[Unit]

/*
Backup
 */
final case class RepoBackupException(override val message : String, override val cause : Cause[Throwable] = Cause.empty)
  extends RepoControllerException(message,cause)
final case class RepoBackupItemException(errorMessage : BackupMessage.Error)
  extends RepoControllerException(s"Error during backup: message=${errorMessage.message}, during=${errorMessage.during}, item=${errorMessage.item}",Cause.empty)

trait RepoBackupData:
  def statusHub : ZStream[Any,Nothing,BackupMessage.Status]
  def awaitSummary : IO[RepoControllerException,BackupMessage.Summary]
  def cancel : UIO[Unit]


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


