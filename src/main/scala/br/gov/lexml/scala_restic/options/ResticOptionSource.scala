package br.gov.lexml.scala_restic.options

import zio.Chunk

trait ResticOptionSource:
  def toArgs : Chunk[String]
