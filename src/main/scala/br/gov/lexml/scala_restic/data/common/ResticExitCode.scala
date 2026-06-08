package br.gov.lexml.scala_restic.data.common

enum ResticExitCode(val ec : Int):
  case REC_SUCCESS extends ResticExitCode(0)
  case REC_FAILURE extends ResticExitCode(1)
  case REC_GO_RUNTIME_FAILURE extends ResticExitCode(2)
  case REC_BACKUP_COULD_NOT_READ extends ResticExitCode(3)
  case REC_INEXISTENT_REPO extends ResticExitCode(10)
  case REC_LOCK_FAILURE extends ResticExitCode(11)
  case REC_WRONG_PASSWORD extends ResticExitCode(12)
  case REC_INTERRUPTED extends ResticExitCode(130)
  case REC_OTHER(override val ec : Int) extends ResticExitCode(ec)

object ResticExitCode:
  def apply(ec : Int): ResticExitCode = ec match {
    case REC_SUCCESS.ec => REC_SUCCESS
    case REC_FAILURE.ec => REC_FAILURE
    case REC_GO_RUNTIME_FAILURE.ec => REC_GO_RUNTIME_FAILURE
    case REC_BACKUP_COULD_NOT_READ.ec => REC_BACKUP_COULD_NOT_READ
    case REC_INEXISTENT_REPO.ec => REC_INEXISTENT_REPO
    case REC_LOCK_FAILURE.ec => REC_LOCK_FAILURE
    case REC_WRONG_PASSWORD.ec => REC_WRONG_PASSWORD
    case REC_INTERRUPTED.ec => REC_INTERRUPTED
    case _ => REC_OTHER(ec)
  }