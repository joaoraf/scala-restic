package br.gov.lexml.scala_restic.options.backup

import br.gov.lexml.scala_restic.options.ResticOptionSource
import br.gov.lexml.scala_restic.options.common.SizeUnit
import zio.Chunk

import java.nio.file.Path
import java.time.ZonedDateTime

enum GroupingOptions(val code : String):
  case ByHost extends GroupingOptions("host")
  case ByPath extends GroupingOptions("path")
  case ByTag extends GroupingOptions("tag")

final case class BackupOptions(
                                dryRun : Boolean = false,
                                exclude : List[String] = List(),
                                excludeCaches : Boolean = false,
                                excludeFromFile : List[Path] = List(),
                                excludeIfPresent : List[String] = List(),
                                excludeLargerThan : Option[(Long,SizeUnit)] = None,
                                filesFrom : List[Path] = List(),
                                filesFromRaw : List[Path] = List(),
                                filesFromVerbatim : List[Path] = List(),
                                force : Boolean = false,
                                groupBy : List[GroupingOptions] = List(),
                                host : Option[String] = None,
                                iexclude : List[String] = List(),
                                iexcludeFromFile : List[Path] = List(),
                                ignoreCtime : Boolean = false,
                                ignoreInode : Boolean = false,
                                noScan : Boolean = false,
                                oneFileSystem : Boolean = false,
                                parent : Option[String] = None,
                                readConcurrency : Option[Int] = None,
                                skipIfUnchanged : Boolean = false,
                                stdin : Boolean = false,
                                stdinFilename : Option[Path] = None,
                                stdinFromCommand : Boolean = false,
                                tag : List[String] = List(),
                                time : Option[ZonedDateTime] = None,
                                withAtime : Boolean = false
                              ) extends ResticOptionSource:
  def toArgs : Chunk[String] =
    val b = Chunk.newBuilder[String]
    if(dryRun) then b += "--dry-run" else ()
    if(exclude.nonEmpty) then b ++= exclude.map(e => s"--exclude=$e") else ()
    if(excludeCaches) then b += "--exclude-caches" else ()
    if(excludeFromFile.nonEmpty) then b ++= excludeFromFile.map(e => s"--exclude-file=$e") else ()
    if(excludeIfPresent.nonEmpty) then b ++= excludeIfPresent.map(e => s"--exclude-if-present=$e") else ()
    if(excludeLargerThan.isDefined) then b += s"--exclude-larger-than=${excludeLargerThan.get._1}${excludeLargerThan.get._2.code}" else ()
    if(filesFrom.nonEmpty) then b ++= filesFrom.map(e => s"--files-from=$e") else ()
    if(filesFromRaw.nonEmpty) then b ++= filesFromRaw.map(e => s"--files-from-raw=$e") else ()
    if(filesFromVerbatim.nonEmpty) then b ++= filesFromVerbatim.map(e => s"--files-from-verbatim=$e") else ()
    if(force) then b += "--force" else ()
    if(groupBy.nonEmpty) then b ++= groupBy.map(e => s"--group-by=${e.code}") else ()
    if(host.isDefined) then b += s"--host=$host" else ()
    if(iexclude.nonEmpty) then b ++= iexclude.map(e => s"--iexclude=$e") else ()
    if(iexcludeFromFile.nonEmpty) then b ++= iexcludeFromFile.map(e => s"--iexclude-file=$e") else ()
    if(ignoreCtime) then b += "--ignore-ctime" else ()
    if(ignoreInode) then b += "--ignore-inode" else ()
    if(noScan) then b += "--no-scan" else ()
    if(oneFileSystem) then b += "--one-file-system" else ()
    if(parent.isDefined) then b += s"--parent=$parent" else ()
    if(readConcurrency.isDefined) then b += s"--read-concurrency=${readConcurrency.get}" else ()
    if(skipIfUnchanged) then b += "--skip-if-unchanged" else ()
    if(stdin) then b += "--stdin" else ()
    if(stdinFilename.isDefined) then b += s"--stdin-filename=$stdinFilename" else ()
    if(stdinFromCommand) then b += "--stdin-from-command" else ()
    if(tag.nonEmpty) then b ++= tag.map(e => s"--tag=$e") else ()
    if(time.isDefined) then b += s"--time=$time" else ()
    if(withAtime) then b += "--with-atime" else ()
    b.result()