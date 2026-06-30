package br.gov.lexml.scala_restic.controller.impl

import br.gov.lexml.scala_restic.command.{ResticCommandBuilderService, ResticCommandService}
import br.gov.lexml.scala_restic.command.impl.ResticCommandServiceImpl
import br.gov.lexml.scala_restic.config.ResticConfig
import br.gov.lexml.scala_restic.options.backup.BackupOptions
import br.gov.lexml.scala_restic.options.common.CommonOptions
import br.gov.lexml.scala_restic.options.forget.ForgetOptionsOverride
import br.gov.lexml.scala_restic.options.restore.RestoreOptions
import br.gov.lexml.scala_restic.options.snapshots.SnapshotsOptions
import org.junit.runner.RunWith
import zio.*
import zio.test.*
import zio.test.junit.ZTestJUnitRunner

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.*

@RunWith(classOf[ZTestJUnitRunner])
final class ResticRepoControllerImplSpec extends ZIOSpecDefault:

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("ResticRepoControllerImpl")(
      test("initializes, backs up, lists snapshots, and restores a temporary repository") {
        withControllerFixture { fixture =>
          for {
            originalContents <- createSourceFiles(fixture.sourceDir)
            existsBefore <- fixture.controller.repoExists(fixture.commonOptions)
            initResult <- fixture.controller.init(fixture.commonOptions)
            existsAfter <- fixture.controller.repoExists(fixture.commonOptions)

            backupProcess <- fixture.controller.backup(
              fixture.commonOptions,
              BackupOptions(tag = List(testTag))
            )
            backupStatusesFiber <- backupProcess.statusStream.runCollect.fork
            backupSummary <- backupProcess.awaitSummary
            _ <- backupStatusesFiber.join
            lateBackupStatuses <- backupProcess.statusStream.runCollect

            snapshots <- fixture.controller.snapshots(
              fixture.commonOptions,
              SnapshotsOptions(tags = Vector(testTag))
            )

            _ <- ZIO.attempt(deleteRecursively(fixture.sourceDir))
            sourceExistsAfterDelete <- ZIO.attempt(Files.exists(fixture.sourceDir))

            restoreProcess <- fixture.controller.restore(
              snapshotID = backupSummary.snapshot_id,
              commonOptions = fixture.commonOptions,
              restoreOptions = RestoreOptions()
            )
            restoreStatusesFiber <- restoreProcess.statusStream.runCollect.fork
            restoreSummary <- restoreProcess.awaitSummary
            _ <- restoreStatusesFiber.join
            lateRestoreStatuses <- restoreProcess.statusStream.runCollect
            restoredContents <- readRelativeFiles(fixture.sourceDir)
          } yield assertTrue(
            !existsBefore,
            initResult.id.nonEmpty,
            existsAfter,
            backupSummary.snapshot_id.nonEmpty,
            snapshots.size == 1,
            snapshots.head.id == backupSummary.snapshot_id,
            !sourceExistsAfterDelete,
            restoreSummary.files_restored >= originalContents.size,
            restoredContents == originalContents,
            lateBackupStatuses.isEmpty,
            lateRestoreStatuses.isEmpty
          )
        }
      },
      test("forgets old snapshots using the configured overrides") {
        withControllerFixture { fixture =>
          for {
            _ <- createSourceFiles(fixture.sourceDir)
            _ <- fixture.controller.init(fixture.commonOptions)
            firstProcess <- fixture.controller.backup(
              fixture.commonOptions,
              BackupOptions(tag = List(forgetTestTag))
            )
            firstStatusesFiber <- firstProcess.statusStream.runCollect.fork
            firstBackup <- firstProcess.awaitSummary
            _ <- firstStatusesFiber.join
            _ <- ZIO.attempt(Files.writeString(fixture.sourceDir.resolve("new-file.txt"), "new snapshot contents"))
            secondProcess <- fixture.controller.backup(
              fixture.commonOptions,
              BackupOptions(tag = List(forgetTestTag))
            )
            secondStatusesFiber <- secondProcess.statusStream.runCollect.fork
            secondBackup <- secondProcess.awaitSummary
            _ <- secondStatusesFiber.join
            _ <- fixture.controller.forget(fixture.commonOptions)
            snapshots <- fixture.controller.snapshots(
              fixture.commonOptions,
              SnapshotsOptions(tags = Vector(forgetTestTag))
            )
          } yield assertTrue(
            firstBackup.snapshot_id != secondBackup.snapshot_id,
            snapshots.map(_.id).toList == List(secondBackup.snapshot_id)
          )
        }
      }
    ) @@ TestAspect.sequential

  private final case class ControllerFixture(
    controller: ResticRepoControllerImpl,
    commonOptions: CommonOptions,
    sourceDir: Path
  )

  private val resticExecutable: Path =
    Path.of(sys.env.getOrElse("RESTIC_TEST_EXECUTABLE", "/usr/bin/restic"))

  private val testTag = "controller-spec"
  private val forgetTestTag = "controller-forget-spec"

  private def withControllerFixture[R, E](
    test: ControllerFixture => ZIO[R, E, TestResult]
  ): ZIO[R, Throwable | E, TestResult] =
    ZIO.scoped {
      for {
        workDir <- tempDirectory("scala-restic-controller-")
        repoPath = workDir.resolve("repo")
        sourceDir = workDir.resolve("source")
        _ <- ZIO.attempt(Files.createDirectories(repoPath))
        service: ResticCommandService = ResticCommandServiceImpl(
          ResticCommandBuilderService(ResticConfig(resticExecutable))
        )
        config = ResticRepoControllerImplConfig(
          name = "test",
          repoPath = repoPath,
          backupRestoreBaseDir = workDir,
          paths = NonEmptyChunk(Path.of("source")),
          backupSkipIfUnchanged = false,
          deleteAfterRestore = false,
          forgetOverrides = ForgetOptionsOverride(keepLast = Some(1))
        )
        controller = ResticRepoControllerImpl(config, service)
        commonOptions = CommonOptions(noCache = true)
        result <- test(ControllerFixture(controller, commonOptions, sourceDir))
      } yield result
    }

  private def tempDirectory(prefix: String): ZIO[Scope, Nothing, Path] =
    ZIO.acquireRelease(
      ZIO.attempt(Files.createTempDirectory(prefix)).orDie
    )(path => ZIO.attempt(deleteRecursively(path)).orDie)

  private def createSourceFiles(root: Path): Task[Map[Path, String]] =
    for {
      _ <- ZIO.attempt(Files.createDirectories(root))
      _ <- ZIO.foreachDiscard(0 until 32) { index =>
        val relativePath = Path.of(s"group-${index % 4}", s"file-$index.txt")
        val content = s"content-$index-${"x" * 4096}"
        ZIO.attempt {
          val file = root.resolve(relativePath)
          Files.createDirectories(file.getParent)
          Files.writeString(file, content, StandardCharsets.UTF_8)
        }
      }
      contents <- readRelativeFiles(root)
    } yield contents

  private def readRelativeFiles(root: Path): Task[Map[Path, String]] =
    ZIO.attempt {
      val stream = Files.walk(root)
      try
        stream.iterator().asScala
          .filter(Files.isRegularFile(_))
          .map(path => root.relativize(path) -> Files.readString(path, StandardCharsets.UTF_8))
          .toMap
      finally stream.close()
    }

  private def deleteRecursively(path: Path): Unit =
    if Files.exists(path) then
      val stream = Files.walk(path)
      try
        stream.iterator().asScala
          .toVector
          .sortBy(_.getNameCount)
          .reverse
          .foreach(Files.deleteIfExists)
      finally stream.close()
