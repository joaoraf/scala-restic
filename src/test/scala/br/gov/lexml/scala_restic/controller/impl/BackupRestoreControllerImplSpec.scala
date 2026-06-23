package br.gov.lexml.scala_restic.controller.impl

import br.gov.lexml.scala_restic.command.{ResticCommandBuilderService, ResticCommandService}
import br.gov.lexml.scala_restic.config.ResticConfig
import br.gov.lexml.scala_restic.controller.BackupRestoreController
import br.gov.lexml.scala_restic.controller.BackupRestoreController.{BackupRequestResult, BackupRestoreHistory, State}
import br.gov.lexml.scala_restic.options.backup.BackupOptions
import br.gov.lexml.scala_restic.options.common.Repo
import cron4s.*
import org.junit.runner.RunWith
import zio.*
import zio.stream.*
import zio.test.*
import zio.test.junit.ZTestJUnitRunner

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.*

@RunWith(classOf[ZTestJUnitRunner])
final class BackupRestoreControllerImplSpec extends ZIOSpecDefault:

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("BackupRestoreControllerImpl")(
      test("initializes a missing repository and performs a manually requested backup") {
        withFixture(initIfNecessary = true) { fixture =>
          for
            sourceFile <- createSourceFile(fixture.sourcePath, "content to back up")
            _ <- awaitState(fixture.controller)(_ == State.WAITING_COMMAND)
            firstResult <- fixture.controller.backupNow
            secondResult <- fixture.controller.backupNow
            completedHistory <- awaitHistory(fixture.controller)(_.backupSummaries.nonEmpty)
            _ <- awaitState(fixture.controller)(_ == State.WAITING_COMMAND)
            repositoryExists <- fixture.service.checkRepositoryExistence(
              Repo.atFolder(fixture.repoPath),
              password = Some(testPassword)
            )
          yield assertTrue(
            Files.exists(sourceFile),
            repositoryExists,
            firstResult == BackupRequestResult.Accepted,
            secondResult == BackupRequestResult.AlreadyRunning,
            completedHistory.backupSummaries.size == 1,
            completedHistory.backupSummaries.head.snapshot_id.nonEmpty
          )
        }
      },
      test("performs a backup when the cron schedule becomes due") {
        withFixture(initIfNecessary = true, backupCronSchedule = everySecondCron) { fixture =>
          for
            _ <- createSourceFile(fixture.sourcePath, "scheduled backup content")
            _ <- awaitState(fixture.controller)(_ == State.WAITING_COMMAND)
            _ <- TestClock.adjust(1.second)
            completedHistory <- awaitHistory(fixture.controller)(_.backupSummaries.nonEmpty)
            _ <- awaitState(fixture.controller)(_ == State.WAITING_COMMAND)
          yield assertTrue(
            completedHistory.backupSummaries.size == 1,
            completedHistory.backupSummaries.head.snapshot_id.nonEmpty
          )
        }
      },
      test("restores the latest snapshot when all configured backup folders are missing") {
        withFixture(
          initIfNecessary = true,
          existingBackupContent = Some("content restored by the controller")
        ) { fixture =>
          val expectedContent = "content restored by the controller"
          for
            restoredHistory <- awaitHistory(fixture.controller)(_.initialRestoreSummary.nonEmpty)
            _ <- awaitState(fixture.controller)(_ == State.WAITING_COMMAND)
            restoredContent <- ZIO.attempt(Files.readString(fixture.sourcePath.resolve("data.txt"), StandardCharsets.UTF_8))
          yield assertTrue(
            restoredHistory.initialRestoreSummary.nonEmpty,
            restoredContent == expectedContent
          )
        }
      },
      test("returns NotReady or Accepted when a backup is requested concurrently with startup") {
        withFixture(initIfNecessary = true) { fixture =>
          for
            // The factory enqueues Start before returning the controller.
            result <- fixture.controller.backupNow
          yield assertTrue(
            result == BackupRequestResult.NotReady || result == BackupRequestResult.Accepted
          )
        }
      },
      test("currentState reports the latest controller state") {
        withFixture(initIfNecessary = true) { fixture =>
          for
            _ <- awaitState(fixture.controller)(_ == State.WAITING_COMMAND)
            afterStart <- fixture.controller.currentState
          yield assertTrue(
            afterStart == State.WAITING_COMMAND
          )
        }
      },
      test("currentHistory reports summaries produced by the controller") {
        withFixture(initIfNecessary = true) { fixture =>
          for
            initialHistory <- fixture.controller.currentHistory
            _ <- createSourceFile(fixture.sourcePath, "history test content")
            _ <- awaitState(fixture.controller)(_ == State.WAITING_COMMAND)
            result <- fixture.controller.backupNow
            expectedHistory <- awaitHistory(fixture.controller)(_.backupSummaries.nonEmpty)
            currentHistory <- fixture.controller.currentHistory
          yield assertTrue(
            initialHistory == BackupRestoreHistory(backupSummariesCapacity = historyCapacity),
            result == BackupRequestResult.Accepted,
            currentHistory == expectedHistory,
            currentHistory.backupSummaries.size == 1
          )
        }
      },
      test("shutdown stops the controller") {
        withFixture(initIfNecessary = true) { fixture =>
          for
            _ <- awaitState(fixture.controller)(_ == State.WAITING_COMMAND)
            _ <- Live.live(fixture.controller.shutdown)
            currentState <- awaitState(fixture.controller)(_ == State.STOPPED())
          yield assertTrue(
            currentState == State.STOPPED()
          )
        }
      },
      test("stops with a failure when the repository is missing and initialization is disabled") {
        withFixture(initIfNecessary = false) { fixture =>
          for
            stopped <- awaitState(fixture.controller) {
              case State.STOPPED(_) => true
              case _ => false
            }
          yield assertTrue(stopped match
            case State.STOPPED(cause) => !cause.isEmpty
            case _ => false
          )
        }
      },
      test("stops with a failure when an existing path is not a Restic repository") {
        withFixture(initIfNecessary = true, createRepositoryPath = true) { fixture =>
          for
            stopped <- awaitState(fixture.controller) {
              case State.STOPPED(_) => true
              case _ => false
            }
          yield assertTrue(stopped match
            case State.STOPPED(cause) => !cause.isEmpty
            case _ => false
          )
        }
      }
    ) @@ TestAspect.sequential

  private final case class Fixture(
    workDir: Path,
    repoPath: Path,
    sourcePath: Path,
    service: ResticCommandService,
    controller: BackupRestoreController
  )

  private val resticExecutable: Path =
    Path.of(sys.env.getOrElse("RESTIC_TEST_EXECUTABLE", "/usr/bin/restic"))

  private val testPassword = "scala-restic-controller-test-password"
  private val historyCapacity = 100

  private val testCron: CronExpr =
    Cron.parse("0 0 0 1 1 ?").fold(throw _, identity)

  private val everySecondCron: CronExpr =
    Cron.parse("* * * * * ?").fold(throw _, identity)

  private def withFixture[R, E](
    initIfNecessary: Boolean,
    restoreIfEmpty: Boolean = true,
    createRepositoryPath: Boolean = false,
    backupCronSchedule: CronExpr = testCron,
    existingBackupContent: Option[String] = None
  )(test: Fixture => ZIO[R, E, TestResult]): ZIO[R & Scope, Throwable | E, TestResult] =
    for
      workDir <- tempDirectory("scala-restic-controller-")
      repoPath = workDir.resolve("repo")
      _ <- ZIO.when(createRepositoryPath)(ZIO.attempt(Files.createDirectories(repoPath)))
      sourcePath = workDir.resolve("source")
      relativeSourcePath = workDir.relativize(sourcePath)
      service = ResticCommandService(ResticCommandBuilderService(ResticConfig(resticExecutable)))
      _ <- ZIO.foreachDiscard(existingBackupContent) { content =>
        for
          _ <- createSourceFile(sourcePath, content)
          _ <- service.init(Repo.atFolder(repoPath), password = Some(testPassword))
          _ <- service.backupSummary(
            Repo.atFolder(repoPath),
            backupOptions = BackupOptions(tag = List("controller-restore-test")),
            password = Some(testPassword),
            basePath = workDir,
            paths = NonEmptyChunk(relativeSourcePath)
          )
          _ <- ZIO.attempt(deleteRecursively(sourcePath))
        yield ()
      }
      config = BackupRestoreControllerConfig(
        resticRepo = ResticRepoConfig(repoPath, testPassword, "localhost"),
        initIfNecessary = initIfNecessary,
        restoreIfEmpty = restoreIfEmpty,
        backupCronSchedule = backupCronSchedule,
        basePath = workDir,
        backupFolders = NonEmptyChunk(relativeSourcePath),
        historyCapacity = historyCapacity
      )
      controller <- BackupRestoreControllerImpl.makeController(config, service)
      result <- test(Fixture(workDir, repoPath, sourcePath, service, controller))
    yield result

  private def awaitState(
    controller: BackupRestoreController
  )(predicate: State => Boolean): ZIO[Any, Throwable, State] =
    Live.live(
      ZStream.repeatZIO(controller.currentState)
        .schedule(Schedule.spaced(10.millis))
        .filter(predicate)
        .runHead
        .someOrFail(new RuntimeException("State stream ended before reaching the expected state"))
        .timeoutFail(new RuntimeException("Timed out waiting for controller state"))(20.seconds)
    )

  private def awaitHistory(
    controller: BackupRestoreController
  )(predicate: BackupRestoreHistory => Boolean): ZIO[Any, Throwable, BackupRestoreHistory] =
    Live.live(
      ZStream.repeatZIO(controller.currentHistory)
        .schedule(Schedule.spaced(10.millis))
        .filter(predicate)
        .runHead
        .someOrFail(new RuntimeException("History stream ended before satisfying the predicate"))
        .timeoutFail(new RuntimeException("Timed out waiting for controller history"))(20.seconds)
    )

  private def createSourceFile(sourcePath: Path, content: String): Task[Path] =
    ZIO.attempt {
      Files.createDirectories(sourcePath)
      val file = sourcePath.resolve("data.txt")
      Files.writeString(file, content, StandardCharsets.UTF_8)
      file
    }

  private def tempDirectory(prefix: String): ZIO[Scope, Nothing, Path] =
    ZIO.acquireRelease(ZIO.attempt(Files.createTempDirectory(prefix)).orDie)(path =>
      ZIO.attempt(deleteRecursively(path)).orDie
    )

  private def deleteRecursively(path: Path): Unit =
    if Files.exists(path) then
      val stream = Files.walk(path)
      try
        stream.iterator().asScala.toVector
          .sortBy(_.toString.length)
          .reverse
          .foreach(Files.deleteIfExists)
      finally stream.close()
