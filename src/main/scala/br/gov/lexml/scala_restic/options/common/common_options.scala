package br.gov.lexml.scala_restic.options.common

import br.gov.lexml.scala_restic.options.ResticOptionSource
import br.gov.lexml.scala_restic.options.common.CompressionMode.AUTO

import java.nio.file.Path
import zio.Chunk

import java.time.Duration

enum SizeUnit(val code : String):
  case B extends SizeUnit("")
  case KB extends SizeUnit("k")
  case MB extends SizeUnit("m")
  case GB extends SizeUnit("g")
  case TB extends SizeUnit("t")

enum CompressionMode(val code : String):
  case AUTO extends CompressionMode("auto")
  case OFF extends CompressionMode("off")
  case MAX extends CompressionMode("max")

object DurationExtensions:
  extension (d : Duration)
    def toResticArg = d.toString.substring(2).toLowerCase

enum Repo:
  case R_LOCATION(location : String)
  case R_FILE(file : Path)

  def toArgs : Chunk[String] = this match
    case R_LOCATION(location) => Chunk(s"--repo=$location")
    case R_FILE(file) => Chunk(s"--repository-file=$file")
object Repo:
  def atURI(repoURI : java.net.URI) : Repo = R_LOCATION(repoURI.toString)
  def atFolder(repoPath : java.io.File) : Repo = R_LOCATION(repoPath.toString)
  def atFolder(repoPath : Path) : Repo = R_LOCATION(repoPath.toString)
  def atLocation(repo : String) : Repo = R_LOCATION(repo)
  def fromFile(repo : Path) : Repo = R_FILE(repo)

final case class CommonOptions(
                              caCert : List[Path] = List(),
                              cacheDir : Option[Path] = None,
                              cleanupCache : Boolean = false,
                              compression : CompressionMode = AUTO,
                              httpUserAgent : String = "",
                              insecureNoPassword : Boolean = false,
                              insecureTLS : Boolean = false,
                              json : Boolean = true,
                              keyHint : String = "",
                              limitDownloadKbps : Double = 0.0,
                              limitUploadKbps : Double = 0.0,
                              noCache : Boolean = false,
                              noExtraVerify : Boolean = false,
                              noLock : Boolean = false,
                              option : List[(String,String)] = List(),
                              packSizeMB : Double = 0.0,
                              passwordCommand : String = "",
                              passwordFile : Option[Path] = None,
                              quiet : Boolean = false,
                              retryLock : Duration = Duration.ZERO,
                              stuckRequestTimeout : Duration = Duration.ofMinutes(5),
                              tlsClientCert : Option[Path] = None,
                              verbose : Int = 0
                              ) extends ResticOptionSource:
  def withJson: CommonOptions = if !json then copy(json = true) else this
  def toArgs : Chunk[String] =
    import DurationExtensions.*
    val b = Chunk.newBuilder[String]
    if(caCert.nonEmpty) then b ++= caCert.map(e => s"--cacert=$e") else ()
    if(cacheDir.isDefined) then b += s"--cache-dir=${cacheDir.get}" else ()
    if(cleanupCache) then b += "--cleanup-cache" else ()
    if(compression != AUTO) then b += s"--compression=${compression.code}" else ()
    if(httpUserAgent.nonEmpty) then b += s"--http-user-agent=$httpUserAgent" else ()
    if(insecureNoPassword) then b += "--insecure-no-password" else ()
    if(insecureTLS) then b += "--insecure-tls" else ()
    if(json) then b += "--json" else ()
    if(keyHint.nonEmpty) then b += s"--key-hint=$keyHint" else ()
    if(limitDownloadKbps > 0.0) then b += s"--limit-download-kbps=$limitDownloadKbps" else ()
    if(limitUploadKbps > 0.0) then b += s"--limit-upload-kbps=$limitUploadKbps" else ()
    if(noCache) then b += "--no-cache" else ()
    if(noExtraVerify) then b += "--no-extra-verify" else ()
    if(noLock) then b += "--no-lock" else ()
    if(option.nonEmpty) then b ++= option.map((k,v) => s"--$k=$v") else ()
    if(packSizeMB > 0.0) then b += s"--pack-size=$packSizeMB" else ()
    if(passwordCommand.nonEmpty) then b += s"--password-command=$passwordCommand" else ()
    if(passwordFile.isDefined) then b += s"--password-file=${passwordFile.get}" else ()
    if(quiet) then b += "--quiet" else ()
    if(retryLock.toMillis > 0) then b += s"--retry-lock=${retryLock.toResticArg}" else ()
    if(tlsClientCert.isDefined) then b += s"--tls-client-cert=${tlsClientCert.get}" else ()
    if(verbose > 0) then b += s"--verbose=$verbose" else ()
    if(stuckRequestTimeout.toMillis > 0) then b += s"--stuck-request-timeout=${stuckRequestTimeout.toResticArg}" else ()
    b.result()
