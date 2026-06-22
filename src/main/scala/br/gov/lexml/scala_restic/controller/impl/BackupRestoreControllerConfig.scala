package br.gov.lexml.scala_restic.controller.impl

import cron4s.CronExpr
import zio.*
import zio.config.magnolia.*

import java.nio.file.Path

final case class ResticRepoControllerConfig(
  resticExecutablePath : Path = Path.of("/usr/bin/restic"),
  hostName : String = "localhost",
  backupRestoreControllers : Vector[BackupRestoreControllerConfig] = Vector()
)

object ResticRepoControllerConfig:
  import br.gov.lexml.scala_restic.misc.ZioConfigInstances.given
  val config: Config[ResticRepoControllerConfig] = deriveConfig[ResticRepoControllerConfig]

final case class ResticRepoConfig(
  repoPath : Path,
  repoPassword : String = "",
  host : String
)
object ResticRepoConfig:
  import br.gov.lexml.scala_restic.misc.ZioConfigInstances.given
  val config: Config[ResticRepoConfig] = deriveConfig[ResticRepoConfig]

final case class BRC_BackupOptions(
  excludes : List[String] = List(),
  iexcludes : List[String] = List(),
  includes : List[String] = List(),
  iincludes : List[String] = List(),
  tags : List[String] = List(),
  oneFileSystem : Boolean = false,
  readConcurrency : Int = 0,
  skipIfChanged : Boolean = false,
  host : String = ""
)

object BRC_BackupOptions:
  import br.gov.lexml.scala_restic.misc.ZioConfigInstances.given
  val config: Config[BRC_BackupOptions] = deriveConfig[BRC_BackupOptions]

final case class BRC_RestoreOptions(
  excludes : List[String] = List(),
  iexcludes : List[String] = List(),
  includes : List[String] = List(),
  iincludes : List[String] = List(),
  tags : List[String] = List(),
  deleteExcluded : Boolean = false,
  hosts : List[String] = List()
)

object BRC_RestoreOptions:
  import br.gov.lexml.scala_restic.misc.ZioConfigInstances.given
  val config: Config[BRC_RestoreOptions] = deriveConfig[BRC_RestoreOptions]

final case class BackupRestoreControllerConfig(
  resticRepo : ResticRepoConfig,
  initIfNecessary : Boolean = true,
  restoreIfEmpty : Boolean = true,
  backupCronSchedule : CronExpr,
  backupFolders : NonEmptyChunk[Path],
  restoreOptions : BRC_RestoreOptions = BRC_RestoreOptions(),
  backupOptions : BRC_BackupOptions = BRC_BackupOptions()
)

object BackupRestoreControllerConfig:
  import br.gov.lexml.scala_restic.misc.ZioConfigInstances.given
  val configs: Config[List[BackupRestoreControllerConfig]] =
    deriveConfig[List[BackupRestoreControllerConfig]].nested("restic","controllers","backup-restore")

