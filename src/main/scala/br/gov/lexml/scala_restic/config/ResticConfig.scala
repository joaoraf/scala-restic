package br.gov.lexml.scala_restic.config

import zio.Config
import zio.config.*
import zio.config.magnolia.*

import java.nio.file.Path

final case class ResticConfig(
  @name("restic-executable-path") resticExecutablePath : Path
)

object ResticConfig:
  import br.gov.lexml.scala_restic.misc.ZioConfigInstances.given
  val config: Config[ResticConfig] = deriveConfig[ResticConfig]