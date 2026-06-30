package br.gov.lexml.scala_restic.options.forget

import br.gov.lexml.scala_restic.options.restore.GroupingOptions
import zio.config.derivation.kebabCase
import zio.*
import zio.config.*
import zio.config.magnolia.*

@kebabCase
final case class ForgetOptionsOverride(
  keepLast : Option[Int] = None,
  keepHourly : Option[Int] = None,
  keepDaily : Option[Int] = None,
  keepWeekly : Option[Int] = None,
  keepMonthly : Option[Int] = None,
  keepYearly : Option[Int] = None,
  keepWithin : Option[String] = None,
  keepWithinHourly : Option[String] = None,
  keepWithinDaily : Option[String] = None,
  keepWithinWeekly : Option[String] = None,
  keepWithinMonthly : Option[String] = None,
  keepWithinYearly : Option[String] = None,
  keepTags : Option[List[String]] = None,
  unsafeAllowRemoveAll : Option[Boolean] = None,
  hosts : Option[List[String]] = None,
  tags : Option[List[String]] = None,
  paths : Option[List[String]] = None,
  compact : Option[Boolean] = None,
  groupBy : Option[List[GroupingOptions]] = None,
  dryRun : Option[Boolean] = None,
  prune : Option[Boolean] = None,
  maxUnused : Option[Long] = None,
  maxRepackSize : Option[Long] = None,
  repackCacheableOnly : Option[Boolean] = None,
  repackSmall : Option[Boolean] = None,
  repackUncompressed : Option[Boolean] = None,
  repackSmallerThan : Option[Long] = None
):
  def overrideOptions(option : ForgetOptions) : ForgetOptions =
    option.copy(
      keepLast = keepLast.getOrElse(option.keepLast),
      keepHourly = keepHourly.getOrElse(option.keepHourly),
      keepDaily = keepDaily.getOrElse(option.keepDaily),
      keepWeekly = keepWeekly.getOrElse(option.keepWeekly),
      keepMonthly = keepMonthly.getOrElse(option.keepMonthly),
      keepYearly = keepYearly.getOrElse(option.keepYearly),
      keepWithin = keepWithin.getOrElse(option.keepWithin),
      keepWithinHourly = keepWithinHourly.getOrElse(option.keepWithinHourly),
      keepWithinDaily = keepWithinDaily.getOrElse(option.keepWithinDaily),
      keepWithinWeekly = keepWithinWeekly.getOrElse(option.keepWithinWeekly),
      keepWithinMonthly = keepWithinMonthly.getOrElse(option.keepWithinMonthly),
      keepWithinYearly = keepWithinYearly.getOrElse(option.keepWithinYearly),
      keepTags = keepTags.getOrElse(option.keepTags),
      unsafeAllowRemoveAll = unsafeAllowRemoveAll.getOrElse(option.unsafeAllowRemoveAll),
      hosts = hosts.getOrElse(option.hosts),
      tags = tags.getOrElse(option.tags),
      paths = paths.getOrElse(option.paths),
      compact = compact.getOrElse(option.compact),
      groupBy = groupBy.getOrElse(option.groupBy),
      dryRun = dryRun.getOrElse(option.dryRun),
      prune = prune.getOrElse(option.prune),
      maxUnused = maxUnused.getOrElse(option.maxUnused),
      maxRepackSize = maxRepackSize.getOrElse(option.maxRepackSize),
      repackCacheableOnly = repackCacheableOnly.getOrElse(option.repackCacheableOnly),
      repackSmall = repackSmall.getOrElse(option.repackSmall),
      repackUncompressed = repackUncompressed.getOrElse(option.repackUncompressed),
      repackSmallerThan = repackSmallerThan.getOrElse(option.repackSmallerThan)
    )

object ForgetOptionsOverride:
  import br.gov.lexml.scala_restic.misc.ZioConfigInstances.given
  given config: Config[ForgetOptionsOverride] = DeriveConfig.derived[ForgetOptionsOverride].desc
