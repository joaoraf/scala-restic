package br.gov.lexml.scala_restic.controller.impl

import br.gov.lexml.scala_restic.options.backup.BackupOptions
import br.gov.lexml.scala_restic.options.common.CommonOptions
import br.gov.lexml.scala_restic.options.forget.{ForgetOptions, ForgetOptionsOverride}
import br.gov.lexml.scala_restic.options.restore.RestoreOptions
import zio.*
import zio.config.*
import zio.config.derivation.kebabCase
import zio.config.magnolia.*

import java.nio.file.Path

@kebabCase
final case class ResticRepoControllerImplConfig(
  name : String = "",
  repoPath : Path,
  backupRestoreBaseDir : Path,
  paths : NonEmptyChunk[Path],
  password : String = "",
  passwordFile : Option[Path] = None,
  host : String = "",
  backupSkipIfUnchanged : Boolean = true,
  readConcurrency : Int = 0,
  deleteAfterRestore : Boolean = true,
  forgetOverrides : ForgetOptionsOverride = ForgetOptionsOverride()
):
  def makeCommonOptions(commonOptions : CommonOptions): CommonOptions =
    commonOptions.copy(
      insecureNoPassword = passwordOption.isEmpty && passwordFile.isEmpty,
      passwordFile = passwordFile,
      verbose = 1,
      json = true
    )

  def makeBackupOptions(backupOptions : BackupOptions) : BackupOptions =
    backupOptions.copy(
      host = Option(host).filter(x => !x.isBlank),
      skipIfUnchanged = backupSkipIfUnchanged,
      readConcurrency = Option(readConcurrency).filter(_ > 0).orElse(backupOptions.readConcurrency)
    )

  def makeRestoreOptions(restoreOptions : RestoreOptions) : RestoreOptions =
    restoreOptions.copy(
      delete = deleteAfterRestore,
      host = List(host).filter(x => !x.isBlank),
      target = Some(backupRestoreBaseDir),
    )

  def makeForgetOptions(forgetOptions : ForgetOptions) : ForgetOptions =
    forgetOverrides.overrideOptions(forgetOptions).copy(
      hosts = List(host).filter(x => !x.isBlank),
    )

  def passwordOption : Option[String] = Option(password).filter(x => !x.isBlank)


object ResticRepoControllerImplConfig:
  import br.gov.lexml.scala_restic.misc.ZioConfigInstances.given
  import ForgetOptionsOverride.given
  val config: Config[ResticRepoControllerImplConfig] = deriveConfig[ResticRepoControllerImplConfig]


final case class ResticRepoControllerImplConfigs(
  controllers : Map[String,ResticRepoControllerImplConfig]
)

object ResticRepoControllerImplConfigs:
  import br.gov.lexml.scala_restic.misc.ZioConfigInstances.given
  val config: Config[ResticRepoControllerImplConfigs] = {
    Config.table("controllers", ResticRepoControllerImplConfig.config).map { m =>
      ResticRepoControllerImplConfigs(m.map { (name, config) => (name, config.copy(name = name)) })
    }.nested("restic")
  }