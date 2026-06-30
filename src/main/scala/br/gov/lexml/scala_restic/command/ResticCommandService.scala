package br.gov.lexml.scala_restic.command

import br.gov.lexml.scala_restic.data.backup.BackupMessage
import br.gov.lexml.scala_restic.data.init.InitResult
import br.gov.lexml.scala_restic.data.restore.RestoreMessage
import br.gov.lexml.scala_restic.data.snapshots.Snapshots
import br.gov.lexml.scala_restic.options.backup.BackupOptions
import br.gov.lexml.scala_restic.options.common.{CommonOptions, Repo}
import br.gov.lexml.scala_restic.options.init.InitOptions
import br.gov.lexml.scala_restic.options.restore.RestoreOptions
import br.gov.lexml.scala_restic.options.snapshots.SnapshotsOptions
import zio.*
import zio.stream.ZStream

import java.nio.file.Path

trait ResticCommandService:
  def checkRepositoryExistence(
    repo: Repo,
    commonOptions: CommonOptions = CommonOptions(),
    password: Option[String] = None
  ): IO[Exception, Boolean]

  def init(
    repo: Repo,
    commonOptions: CommonOptions = CommonOptions(),
    initOptions: InitOptions = InitOptions(),
    password: Option[String] = None
  ): IO[Exception, InitResult]

  def backupStream(
    repo: Repo,
    commonOptions: CommonOptions = CommonOptions(),
    backupOptions: BackupOptions = BackupOptions(),
    password: Option[String] = None,
    basePath: Path,
    paths: NonEmptyChunk[Path]
  ): IO[Exception, ZStream[Any, Exception, BackupMessage]]

  def backupSummary(
    repo: Repo,
    commonOptions: CommonOptions = CommonOptions(),
    backupOptions: BackupOptions = BackupOptions(),
    password: Option[String] = None,
    basePath: Path,
    paths: NonEmptyChunk[Path]
  ): IO[Exception, BackupMessage.Summary]

  def restoreStream(
    repo: Repo,
    commonOptions: CommonOptions = CommonOptions(),
    restoreOptions: RestoreOptions = RestoreOptions(),
    password: Option[String] = None,
    snapshotID: String
  ): IO[Exception, ZStream[Any, Exception, RestoreMessage]]

  def restoreSummary(
    repo: Repo,
    commonOptions: CommonOptions = CommonOptions(),
    restoreOptions: RestoreOptions = RestoreOptions(),
    password: Option[String] = None,
    snapshotID: String
  ): IO[Exception, RestoreMessage.Summary]

  def snapshots(
    repo: Repo,
    commonOptions: CommonOptions = CommonOptions(),
    snapshotsOptions: SnapshotsOptions,
    password: Option[String] = None
  ): IO[Exception, Snapshots]
