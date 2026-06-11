package br.gov.lexml.scala_restic.repository

import br.gov.lexml.scala_restic.data.backup.BackupMessage
import br.gov.lexml.scala_restic.data.restore.RestoreMessage
import br.gov.lexml.scala_restic.options.backup.BackupOptions
import br.gov.lexml.scala_restic.options.restore.RestoreOptions
import zio.*
import zio.stream.*

import java.nio.file.Path

trait ResticRepository /*:
  def repoExists : ZIO[Any,Exception,Boolean]
  def init : ZIO[Any,Exception,Unit]
  def backup(backupOptions : BackupOptions = BackupOptions(),paths : Path*) : ZIO[Any,Exception,ZStream[Any,Exception,BackupMessage]]
  def restore(restoreOptions : RestoreOptions = RestoreOptions(), snapshotId : String) : ZIO[Any,Exception,ZStream[Any,Exception,RestoreMessage]]
  */
