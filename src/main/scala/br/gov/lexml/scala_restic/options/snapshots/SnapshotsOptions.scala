package br.gov.lexml.scala_restic.options.snapshots

import br.gov.lexml.scala_restic.options.ResticOptionSource
import zio.*

final case class SnapshotsOptions(
  compact : Boolean = false,
  groupBy : String = "",
  host : Vector[String] = Vector(),
  latest : Int = 0,
  path : Vector[String] = Vector(),
  tags : Vector[String] = Vector(),
) extends ResticOptionSource:
  override def toArgs : Chunk[String] =
    val b = Chunk.newBuilder[String]
    if(compact) then b += "--compact" else ()
    if(groupBy.nonEmpty) then b += s"--group-by=$groupBy" else ()
    if(host.nonEmpty) then b ++= host.map(e => s"--host=$e") else ()
    if(latest > 0) then b += s"--latest=$latest" else ()
    if(path.nonEmpty) then b ++= path.map(e => s"--path=$e") else ()
    if(tags.nonEmpty) then b ++= tags.map(e => s"--tag=$e") else ()
    b.result()




