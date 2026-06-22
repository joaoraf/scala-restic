package br.gov.lexml.scala_restic.controller.impl

import br.gov.lexml.scala_restic.command.{ResticCommandBuilderService, ResticCommandService}
import br.gov.lexml.scala_restic.config.ResticConfig
import br.gov.lexml.scala_restic.controller.impl.BackupRestoreControllerImpl.*
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
    suite("BackupRestoreControllerImpl2")(
      test("initializes a missing repository and performs a manually requested backup") {
        withFixture(initIfNecessary = true) { fixture =>
          for
            sourceFile <- createSourceFile(fixture.sourcePath, "content to back up")
            _ <- fixture.controller.start
            _ <- awaitState(fixture.state)(_ == State.WAITING_COMMAND)
            firstReply <- Promise.make[Nothing, BackupRequestResult]
            secondReply <- Promise.make[Nothing, BackupRequestResult]
            _ <- fixture.commands.offerAll(Chunk(Command.BackupNow(firstReply), Command.BackupNow(secondReply)))
            firstResult <- awaitPromise(firstReply)
            secondResult <- awaitPromise(secondReply)
            completedHistory <- awaitHistory(fixture.history)(_.backupSummaries.nonEmpty)
            _ <- awaitState(fixture.state)(_ == State.WAITING_COMMAND)
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
            _ <- fixture.controller.start
            _ <- awaitState(fixture.state)(_ == State.WAITING_COMMAND)
            _ <- TestClock.adjust(1.second)
            completedHistory <- awaitHistory(fixture.history)(_.backupSummaries.nonEmpty)
            _ <- awaitState(fixture.state)(_ == State.WAITING_COMMAND)
          yield assertTrue(
            completedHistory.backupSummaries.size == 1,
            completedHistory.backupSummaries.head.snapshot_id.nonEmpty
          )
        }
      },
      test("restores the latest snapshot when all configured backup folders are missing") {
        withFixture(initIfNecessary = true, restoreIfEmpty = true) { fixture =>
          val expectedContent = "content restored by the controller"
          for
            sourceFile <- createSourceFile(fixture.sourcePath, expectedContent)
            _ <- fixture.service.init(Repo.atFolder(fixture.repoPath), password = Some(testPassword))
            backup <- fixture.service.backupSummary(
              Repo.atFolder(fixture.repoPath),
              backupOptions = BackupOptions(tag = List("controller-restore-test")),
              password = Some(testPassword),
              paths = NonEmptyChunk(fixture.sourcePath)
            )
            _ <- ZIO.attempt(deleteRecursively(fixture.sourcePath))
            _ <- fixture.controller.start
            restoredHistory <- awaitHistory(fixture.history)(_.initialRestoreSummary.nonEmpty)
            _ <- awaitState(fixture.state)(_ == State.WAITING_COMMAND)
            restoredContent <- ZIO.attempt(Files.readString(sourceFile, StandardCharsets.UTF_8))
          yield assertTrue(
            backup.snapshot_id.nonEmpty,
            restoredHistory.initialRestoreSummary.nonEmpty,
            restoredContent == expectedContent
          )
        }
      },
      test("rejects backup requests while startup is still in progress") {
        withFixture(initIfNecessary = true) { fixture =>
          for
            reply <- Promise.make[Nothing, BackupRequestResult]
            _ <- fixture.controller.start
            // Start was enqueued first, so this request is handled in STARTING before Initialize.
            _ <- fixture.commands.offer(Command.BackupNow(reply))
            result <- awaitPromise(reply)
          yield assertTrue(result == BackupRequestResult.NotReady)
        }
      },
      test("currentState reports the latest controller state") {
        withFixture(initIfNecessary = true) { fixture =>
          for
            beforeStart <- fixture.controller.currentState
            _ <- fixture.controller.start
            _ <- awaitState(fixture.state)(_ == State.WAITING_COMMAND)
            afterStart <- fixture.controller.currentState
          yield assertTrue(
            beforeStart == State.NOT_STARTED,
            afterStart == State.WAITING_COMMAND
          )
        }
      },
      test("currentHistory reports summaries produced by the controller") {
        withFixture(initIfNecessary = true) { fixture =>
          for
            initialHistory <- fixture.controller.currentHistory
            _ <- createSourceFile(fixture.sourcePath, "history test content")
            _ <- fixture.controller.start
            _ <- awaitState(fixture.state)(_ == State.WAITING_COMMAND)
            reply <- Promise.make[Nothing, BackupRequestResult]
            _ <- fixture.commands.offer(Command.BackupNow(reply))
            result <- awaitPromise(reply)
            expectedHistory <- awaitHistory(fixture.history)(_.backupSummaries.nonEmpty)
            currentHistory <- fixture.controller.currentHistory
          yield assertTrue(
            initialHistory == BackupRestoreHistory(),
            result == BackupRequestResult.Accepted,
            currentHistory == expectedHistory,
            currentHistory.backupSummaries.size == 1
          )
        }
      },
      test("shutdown stops the controller and interrupts its command fiber") {
        withFixture(initIfNecessary = true) { fixture =>
          for
            _ <- fixture.controller.start
            _ <- awaitState(fixture.state)(_ == State.WAITING_COMMAND)
            _ <- Live.live(fixture.controller.shutdown)
            currentState <- fixture.controller.currentState
            commandFiber <- fixture.commandFiberPromise.await
            commandFiberExit <- commandFiber.poll
          yield assertTrue(
            currentState == State.STOPPED(),
            commandFiberExit.nonEmpty
          )
        }
      },
      test("stops with a failure when the repository is missing and initialization is disabled") {
        withFixture(initIfNecessary = false) { fixture =>
          for
            _ <- fixture.controller.start
            stopped <- awaitState(fixture.state) {
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
            _ <- fixture.controller.start
            stopped <- awaitState(fixture.state) {
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
    commands: Queue[Command],
    state: SubscriptionRef[State],
    history: SubscriptionRef[BackupRestoreHistory],
    commandFiberPromise: Promise[Nothing, Fiber[Throwable, Unit]],
    controller: BackupRestoreControllerImpl
  )

  private val resticExecutable: Path =
    Path.of(sys.env.getOrElse("RESTIC_TEST_EXECUTABLE", "/usr/bin/restic"))

  private val testPassword = "scala-restic-controller-test-password"

  private val testCron: CronExpr =
    Cron.parse("0 0 0 1 1 ?").fold(throw _, identity)

  private val everySecondCron: CronExpr =
    Cron.parse("* * * * * ?").fold(throw _, identity)

  private def withFixture[R, E](
    initIfNecessary: Boolean,
    restoreIfEmpty: Boolean = true,
    createRepositoryPath: Boolean = false,
    backupCronSchedule: CronExpr = testCron
  )(test: Fixture => ZIO[R, E, TestResult]): ZIO[R & Scope, Throwable | E, TestResult] =
    for
      workDir <- tempDirectory("scala-restic-controller-")
      repoPath = workDir.resolve("repo")
      _ <- ZIO.when(createRepositoryPath)(ZIO.attempt(Files.createDirectories(repoPath)))
      sourcePath = workDir.resolve("source")
      service = ResticCommandService(ResticCommandBuilderService(ResticConfig(resticExecutable)))
      commands <- Queue.unbounded[Command]
      state <- SubscriptionRef.make[State](State.NOT_STARTED)
      history <- SubscriptionRef.make(BackupRestoreHistory())
      commandFiber <- Promise.make[Nothing, Fiber[Throwable, Unit]]
      config = BackupRestoreControllerConfig(
        resticRepo = ResticRepoConfig(repoPath, testPassword, "localhost"),
        initIfNecessary = initIfNecessary,
        restoreIfEmpty = restoreIfEmpty,
        backupCronSchedule = backupCronSchedule,
        backupFolders = NonEmptyChunk(sourcePath)
      )
      controller = BackupRestoreControllerImpl(
        config,
        commands,
        state,
        history,
        commandFiber,
        service
      )
      result <- test(Fixture(workDir, repoPath, sourcePath, service, commands, state, history, commandFiber, controller))
    yield result

  private def awaitState(
    state: SubscriptionRef[State]
  )(predicate: State => Boolean): ZIO[Any, Throwable, State] =
    Live.live(
      state.changes
        .filter(predicate)
        .runHead
        .someOrFail(new RuntimeException("State stream ended before reaching the expected state"))
        .timeoutFail(new RuntimeException("Timed out waiting for controller state"))(20.seconds)
    )

  private def awaitHistory(
    history: SubscriptionRef[BackupRestoreHistory]
  )(predicate: BackupRestoreHistory => Boolean): ZIO[Any, Throwable, BackupRestoreHistory] =
    Live.live(
      history.changes
        .filter(predicate)
        .runHead
        .someOrFail(new RuntimeException("History stream ended before satisfying the predicate"))
        .timeoutFail(new RuntimeException("Timed out waiting for controller history"))(20.seconds)
    )

  private def awaitPromise[A](promise: Promise[Nothing, A]): ZIO[Any, Throwable, A] =
    Live.live(promise.await.timeoutFail(new RuntimeException("Timed out waiting for command reply"))(20.seconds))

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
