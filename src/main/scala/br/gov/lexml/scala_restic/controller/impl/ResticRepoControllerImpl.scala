package br.gov.lexml.scala_restic.controller.impl

import br.gov.lexml.scala_restic.controller.ResticRepoController
import zio.*
import zio.stream.*
import zio.config.*
import zio.config.magnolia.*

import java.time.{Duration => JavaDuration, *}
import java.nio.file.Path

import ResticRepoState.*
import RepoBehaviour.*

final case class ResticRepoControllerConfig(
  resticExecutablePath : Path = Path.of("/usr/bin/restic"),
  hostName : String = "localhost",
)

object ResticRepoControllerConfig:
  import br.gov.lexml.scala_restic.misc.ZioConfigInstances.given
  val config: Config[ResticRepoControllerConfig] = deriveConfig[ResticRepoControllerConfig]

enum RepoBehaviour:
  case RB_RESTORE_ONLY
  case RB_BACKUP_RESTORE

enum ResticRepoState:
  case RRS_STARTING
  case RRS_NOT_INITIALIZED
  case RRS_INITIALIZING
  case RRS_READY(repoId : String)
  case RRS_BACKING_UP
  case RRS_RESTORING

final case class NotStartedState()

final case class BasicRepoData(
  repoPath : Path,
  repoPassword : String = "",
)

final case class RepoSchedulerState(
  lastBackup : Option[Instant] = None,
  lastRestore : Option[Instant] = None,
  nextScheduledBackup : Option[Instant] = None,
  nextScheduledRestore : Option[Instant] = None,
)

final case class ResticRepo(
  basicData : BasicRepoData,
  behaviour : RepoBehaviour,
  repoState : ResticRepoState = RRS_STARTING,
  schedulerState : RepoSchedulerState = RepoSchedulerState(),
)

class ResticRepoControllerImpl(config : ResticRepoControllerConfig) extends ResticRepoController:


