package br.gov.lexml.scala_restic.options.forget

import br.gov.lexml.scala_restic.options.restore.GroupingOptions
import org.junit.runner.RunWith
import zio.test.*
import zio.test.junit.ZTestJUnitRunner

@RunWith(classOf[ZTestJUnitRunner])
final class ForgetOptionsOverrideSpec extends ZIOSpecDefault:

  override def spec =
    suite("ForgetOptionsOverride")(
      test("overrides Some fields and preserves None fields") {
        val original = ForgetOptions(
          keepLast = 1,
          keepHourly = 2,
          keepDaily = 3,
          keepWeekly = 4,
          keepMonthly = 5,
          keepYearly = 6,
          keepWithin = "7d",
          keepWithinHourly = "8h",
          keepWithinDaily = "9d",
          keepWithinWeekly = "10w",
          keepWithinMonthly = "11m",
          keepWithinYearly = "12y",
          keepTags = List("keep-original"),
          unsafeAllowRemoveAll = true,
          hosts = List("original-host"),
          tags = List("original-tag"),
          paths = List("/original"),
          compact = true,
          groupBy = List(GroupingOptions.ByHost),
          dryRun = true,
          prune = true,
          maxUnused = 13L,
          maxRepackSize = 14L,
          repackCacheableOnly = true,
          repackSmall = true,
          repackUncompressed = true,
          repackSmallerThan = 15L
        )
        val overrides = ForgetOptionsOverride(
          keepLast = Some(101),
          keepDaily = Some(103),
          keepMonthly = Some(105),
          keepWithin = Some("107d"),
          keepWithinDaily = Some("109d"),
          keepWithinMonthly = Some("111m"),
          keepTags = Some(List("keep-override")),
          hosts = Some(List("override-host")),
          paths = Some(List("/override")),
          groupBy = Some(List(GroupingOptions.ByPath)),
          prune = Some(false),
          maxRepackSize = Some(114L),
          repackSmall = Some(false),
          repackSmallerThan = Some(115L)
        )

        assertTrue(
          overrides.overrideOptions(original) == original.copy(
            keepLast = 101,
            keepDaily = 103,
            keepMonthly = 105,
            keepWithin = "107d",
            keepWithinDaily = "109d",
            keepWithinMonthly = "111m",
            keepTags = List("keep-override"),
            hosts = List("override-host"),
            paths = List("/override"),
            groupBy = List(GroupingOptions.ByPath),
            prune = false,
            maxRepackSize = 114L,
            repackSmall = false,
            repackSmallerThan = 115L
          )
        )
      }
    )
