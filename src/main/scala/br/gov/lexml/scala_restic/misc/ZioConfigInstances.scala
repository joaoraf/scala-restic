package br.gov.lexml.scala_restic.misc

import zio.*
import zio.config.*
import zio.config.magnolia.*

import java.nio.file.Path

object ZioConfigInstances:
  given pathConfig : Config[Path] =
    Config.string.mapAttempt(Path.of(_))

