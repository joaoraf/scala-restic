package br.gov.lexml.scala_restic.options.forget

import br.gov.lexml.scala_restic.options.ResticOptionSource
import br.gov.lexml.scala_restic.options.restore.GroupingOptions
import zio.*

final case class ForgetOptions(
  keepLast : Int = 0,
  keepHourly : Int = 0,
  keepDaily : Int = 0,
  keepWeekly : Int = 0,
  keepMonthly : Int = 0,
  keepYearly : Int = 0,
  keepWithin : String = "",
  keepWithinHourly : String = "",
  keepWithinDaily : String = "",
  keepWithinWeekly : String = "",
  keepWithinMonthly : String = "",
  keepWithinYearly : String = "",
  keepTags : List[String] = List(),
  unsafeAllowRemoveAll : Boolean = false,
  hosts : List[String] = List(),
  tags : List[String] = List(),
  paths : List[String] = List(),
  compact : Boolean = false,
  groupBy : List[GroupingOptions] = List(),
  dryRun : Boolean = false,
  prune : Boolean = false,
  maxUnused : Long = 0L,
  maxRepackSize : Long = 0L,
  repackCacheableOnly : Boolean = false,
  repackSmall : Boolean = false,
  repackUncompressed : Boolean = false,
  repackSmallerThan : Long = 0L
) extends ResticOptionSource:
  override def toArgs : Chunk[String] =
    val b = Chunk.newBuilder[String]
    if keepLast != 0 then b += s"--keep-last=$keepLast" else ()
    if keepHourly != 0 then b += s"--keep-hourly=$keepHourly" else ()
    if keepDaily != 0 then b += s"--keep-daily=$keepDaily" else ()
    if keepWeekly != 0 then b += s"--keep-weekly=$keepWeekly" else ()
    if keepMonthly != 0 then b += s"--keep-monthly=$keepMonthly" else ()
    if keepYearly != 0 then b += s"--keep-yearly=$keepYearly" else ()
    if keepWithin.nonEmpty then b += s"--keep-within=$keepWithin" else ()
    if keepWithinHourly.nonEmpty then b += s"--keep-within-hourly=$keepWithinHourly" else ()
    if keepWithinDaily.nonEmpty then b += s"--keep-within-daily=$keepWithinDaily" else ()
    if keepWithinWeekly.nonEmpty then b += s"--keep-within-weekly=$keepWithinWeekly" else ()
    if keepWithinMonthly.nonEmpty then b += s"--keep-within-monthly=$keepWithinMonthly" else ()
    if keepWithinYearly.nonEmpty then b += s"--keep-within-yearly=$keepWithinYearly" else ()
    if keepTags.nonEmpty then b ++= keepTags.map(e => s"--keep-tag=$e") else ()
    if unsafeAllowRemoveAll then b += "--unsafe-allow-remove-all" else ()
    if hosts.nonEmpty then b ++= hosts.map(e => s"--host=$e") else ()
    if tags.nonEmpty then b ++= tags.map(e => s"--tag=$e") else ()
    if paths.nonEmpty then b ++= paths.map(e => s"--path=$e") else ()
    if compact then b += "--compact" else ()
    if groupBy.nonEmpty then b ++= groupBy.map(e => s"--group-by=${e.code}") else ()
    if dryRun then b += "--dry-run" else ()
    if prune then b += "--prune" else ()
    if maxUnused != 0L then b += s"--max-unused=$maxUnused" else ()
    if maxRepackSize != 0L then b += s"--max-repack-size=$maxRepackSize" else ()
    if repackCacheableOnly then b += "--repack-cacheable-only" else ()
    if repackSmall then b += "--repack-small" else ()
    if repackUncompressed then b += "--repack-uncompressed" else ()
    if repackSmallerThan != 0L then b += s"--repack-smaller-than=$repackSmallerThan" else ()
    b.result()
