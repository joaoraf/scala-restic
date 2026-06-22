package br.gov.lexml.scala_restic.controller


import zio.*
import zio.stream.*

trait BackupRestoreController:
  def start : Task[Unit]
  def backupNow : UIO[Boolean]
