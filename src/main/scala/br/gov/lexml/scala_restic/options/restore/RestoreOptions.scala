package br.gov.lexml.scala_restic.options.restore

import br.gov.lexml.scala_restic.options.ResticOptionSource
import br.gov.lexml.scala_restic.options.common.SizeUnit
import zio.Chunk

import java.nio.file.Path
import java.time.ZonedDateTime

enum GroupingOptions(val code : String):
  case ByHost extends GroupingOptions("host")
  case ByPath extends GroupingOptions("path")
  case ByTag extends GroupingOptions("tag")

enum OverwriteBehavior(val code : String):
  case ALWAYS extends OverwriteBehavior("always")
  case IF_CHANGED extends OverwriteBehavior("if-changed")
  case IF_NEWER extends OverwriteBehavior("if-newer")
  case NEVER extends OverwriteBehavior("never")

final case class RestoreOptions(
                                delete : Boolean = false, //NEW
                                dryRun : Boolean = false,
                                exclude : List[String] = List(),
                                excludeFromFile : List[Path] = List(),
                                excludeXattr : List[String] = List(),
                                host : List[String] = List(),
                                iexclude : List[String] = List(),
                                iexcludeFromFile : List[Path] = List(),
                                iinclude : List[String] = List(),
                                iincludeFromFile : List[Path] = List(),
                                include : List[String] = List(),
                                includeFromFile : List[Path] = List(),
                                includeXattr : List[String] = List(),
                                overwrite : OverwriteBehavior = OverwriteBehavior.ALWAYS,
                                path : List[Path] = List(),
                                sparse : Boolean = false,
                                tag : List[String] = List(),
                                target : Option[Path] = None,
                                verify : Boolean = false,
                               ) extends ResticOptionSource:
  override def toArgs : Chunk[String] =
    val b = Chunk.newBuilder[String]
    if(delete) then b += "--delete" else ()
    if(dryRun) then b += "--dry-run" else ()
    if(exclude.nonEmpty) then b ++= exclude.map(e => s"--exclude=$e") else ()
    if(excludeFromFile.nonEmpty) then b ++= excludeFromFile.map(e => s"--exclude-file=$e") else ()
    if(excludeXattr.nonEmpty) then b ++= excludeXattr.map(e => s"--exclude-xattr=$e") else ()
    if(host.nonEmpty) then b ++= host.map(e => s"--host=$e") else ()
    if(iexclude.nonEmpty) then b ++= iexclude.map(e => s"--iexclude=$e") else ()
    if(iexcludeFromFile.nonEmpty) then b ++= iexcludeFromFile.map(e => s"--iexclude-file=$e") else ()
    if(iinclude.nonEmpty) then b ++= iinclude.map(e => s"--iinclude=$e") else ()
    if(iincludeFromFile.nonEmpty) then b ++= iincludeFromFile.map(e => s"--iinclude-file=$e") else ()
    if(include.nonEmpty) then b ++= include.map(e => s"--include=$e") else ()
    if(includeFromFile.nonEmpty) then b ++= includeFromFile.map(e => s"--include-file=$e") else ()
    if(includeXattr.nonEmpty) then b ++= includeXattr.map(e => s"--include-xattr=$e") else ()
    if(overwrite != OverwriteBehavior.ALWAYS) then b += s"--overwrite=${overwrite.code}" else ()
    if(path.nonEmpty) then b ++= path.map(e => s"--path=$e") else ()
    if(sparse) then b += "--sparse" else ()
    if(tag.nonEmpty) then b ++= tag.map(e => s"--tag=$e") else ()
    if(target.isDefined) then b += s"--target=${target.get}" else ()
    if(verify) then b += "--verify" else ()
    b.result()
