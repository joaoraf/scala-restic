package br.gov.lexml.scala_restic.controller

import br.gov.lexml.scala_restic.controller.impl.BackupRestoreEvent
import zio.*
import zio.stream.*

trait BackupRestoreController:
  def start : Task[Unit]
  def backupNow : UIO[Boolean]
  def subscribe: ZIO[Scope, Nothing, Dequeue[BackupRestoreEvent]]