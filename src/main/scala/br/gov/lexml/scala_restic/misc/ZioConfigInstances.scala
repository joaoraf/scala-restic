package br.gov.lexml.scala_restic.misc

import zio.*
import zio.config.*
import zio.config.magnolia.*

import java.nio.file.Path
import cron4s.*

object ZioConfigInstances:
  given pathConfig : Config[Path] =
    Config.string.mapAttempt(Path.of(_))
  given cronExprConfig : Config[CronExpr] =
    Config.string.mapOrFail(expr =>
      Cron.parse(expr).fold(err => Left(Config.Error.InvalidData(Chunk(expr),message = s"Error parsing cron expression: ${err.getMessage}")),x => Right(x))
    )

