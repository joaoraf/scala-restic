package br.gov.lexml.scala_restic.data.common

import zio.schema.*
import java.nio.file.{Path, Paths}

object CommonSchemas:
  given pathSchema: Schema[Path] = Schema.primitive[String].transform(
    txt => Paths.get(txt),
    _.toString
  )