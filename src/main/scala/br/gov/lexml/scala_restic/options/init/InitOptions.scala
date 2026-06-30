package br.gov.lexml.scala_restic.options.init

import br.gov.lexml.scala_restic.options.ResticOptionSource
import br.gov.lexml.scala_restic.options.init.RepositoryVersion.RV_STABLE
import zio.Chunk

import java.nio.file.Path

enum RepositoryVersion:
  case RV_STABLE, RV_LATEST
  case RV_VERSION(version : String)
  
  def toArgValue: String = this match {
    case RV_STABLE => "stable"
    case RV_LATEST => "latest"
    case RV_VERSION(version) => version
  }

final case class InitOptions(
  copyChunkerParams : Boolean = false,
  fromInsecureNoPassword : Boolean = false,
  fromKeyHint : String = "",
  fromPasswordCommand : String = "",
  fromPasswordFile : Option[Path] = None,
  fromRepositoryFile : Option[Path] = None,
  repositoryVersion : RepositoryVersion = RV_STABLE
) extends ResticOptionSource:
  override def toArgs : Chunk[String] =
    val b = Chunk.newBuilder[String]
    if(copyChunkerParams) { b += "--copy-chunker-params" }
    if(fromInsecureNoPassword) { b += "--from-insecure-no-password" }
    if(fromKeyHint.nonEmpty) { b += s"--from-key-hint=$fromKeyHint" }
    if(fromPasswordCommand.nonEmpty) { b += s"--from-password-command=$fromPasswordCommand" }
    if(fromPasswordFile.isDefined) { b += s"--from-password-file=$fromPasswordFile" }
    if(fromRepositoryFile.isDefined) { b += s"--from-repository-file=$fromRepositoryFile" }
    if(repositoryVersion != RV_STABLE) { b += s"--repository-version=${repositoryVersion.toArgValue}" }
    b.result()
    
