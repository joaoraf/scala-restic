package br.gov.lexml.scala_restic.command

import br.gov.lexml.scala_restic.config.ResticConfig
import br.gov.lexml.scala_restic.command.impl.ResticCommandServiceImpl
import br.gov.lexml.scala_restic.data.backup.BackupMessage
import br.gov.lexml.scala_restic.data.restore.RestoreMessage
import br.gov.lexml.scala_restic.options.backup.BackupOptions
import br.gov.lexml.scala_restic.options.common.{CommonOptions, Repo}
import br.gov.lexml.scala_restic.options.forget.ForgetOptions
import br.gov.lexml.scala_restic.options.restore.RestoreOptions
import br.gov.lexml.scala_restic.options.snapshots.SnapshotsOptions
import org.junit.runner.RunWith
import zio.*
import zio.logging.LogFilter.LogLevelByNameConfig
import zio.test.*
import zio.test.Assertion.*
import zio.test.junit.ZTestJUnitRunner

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.*
import zio.logging.{ConsoleLoggerConfig, LogFilter, LogFormat, consoleLogger}

@RunWith(classOf[ZTestJUnitRunner])
final class ResticCommandServiceSpec extends ZIOSpecDefault:

  override val bootstrap: ZLayer[Any, Any, TestEnvironment] =
    ZLayer.make[TestEnvironment](
      testEnvironment,
      Runtime.removeDefaultLoggers,
      consoleLogger(ConsoleLoggerConfig(
        LogFormat.default,
        LogLevelByNameConfig(LogLevel.Debug)
      ))
    )

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("ResticCommandService")(
      test("initializes a repository and verifies that it exists") {
        withResticFixture { fixture =>
          for {
            existsBefore <- fixture.service.checkRepositoryExistence(fixture.repo, fixture.commonOptions)
            initResult <- fixture.service.init(fixture.repo, fixture.commonOptions)
            existsAfter <- fixture.service.checkRepositoryExistence(fixture.repo, fixture.commonOptions)
          } yield assertTrue(!existsBefore) &&
            assertTrue(initResult.id.nonEmpty) &&
            assertTrue(initResult.repository.nonEmpty) &&
            assertTrue(existsAfter)
        }
      },
      test("backs up temporary random files and restores them") {
        withResticFixture { fixture =>
          for {
            source <- createRandomSourceTree(
              fixture.workDir.resolve("source"),
              maxFolderDepth = 3,
              maxFolderSize = 4,
              numberOfFiles = 10,
              totalSizeBytes = 4096
            )
            relativeSource = fixture.workDir.relativize(source.root)
            restoreTarget <- ZIO.attempt(Files.createDirectories(fixture.workDir.resolve("restore-target")))
            _ <- fixture.service.init(fixture.repo, fixture.commonOptions)
            backupSummary <- fixture.service.backupSummary(
              fixture.repo,
              fixture.commonOptions,
              BackupOptions(tag = List("zio-test")),
              basePath = fixture.workDir,
              paths = NonEmptyChunk(relativeSource)
            )
            restoreSummary <- fixture.service.restoreSummary(
              fixture.repo,
              fixture.commonOptions,
              RestoreOptions(target = Some(restoreTarget)),
              snapshotID = s"${backupSummary.snapshot_id}:$relativeSource"
            )
            restoredContents <- readRelativeFiles(restoreTarget)
          } yield assert(backupSummary.snapshot_id)(isNonEmptyString) &&
            assertTrue(backupSummary.total_files_processed >= source.contents.size) &&
            assertTrue(restoreSummary.files_restored >= source.contents.size) &&
            assertTrue(restoredContents == source.contents)
        }
      },
      test("streams backup and restore messages including summaries") {
        withResticFixture { fixture =>
          for {
            source <- createRandomSourceTree(
              fixture.workDir.resolve("stream-source"),
              maxFolderDepth = 3,
              maxFolderSize = 10,
              numberOfFiles = 600,
              totalSizeBytes = 2048L * 1024L
            )
            relativeSource = fixture.workDir.relativize(source.root)
            restoreTarget <- ZIO.attempt(Files.createDirectories(fixture.workDir.resolve("stream-restore-target")))
            _ <- fixture.service.init(fixture.repo, fixture.commonOptions)
            backupStream <- fixture.service.backupStream(
              fixture.repo,
              fixture.commonOptions.copy(verbose = 2),
              basePath = fixture.workDir,
              paths = NonEmptyChunk(relativeSource)
            )
            backupMessages <- backupStream.tap(logResticMessage("backup")).runCollect
            snapshotId <- ZIO.fromOption(backupMessages.collectFirst { case summary: BackupMessage.Summary => summary.snapshot_id })
              .mapError(_ => ResticException("Backup stream did not contain a summary"))
            restoreStream <- fixture.service.restoreStream(
              fixture.repo,
              fixture.commonOptions,
              RestoreOptions(target = Some(restoreTarget)),
              snapshotID = s"$snapshotId:$relativeSource"
            )
            restoreMessages <- restoreStream.tap(logResticMessage("restore")).runCollect
          } yield assertTrue(backupMessages.exists(_.isInstanceOf[BackupMessage.Summary])) &&
            assertTrue(restoreMessages.exists(_.isInstanceOf[RestoreMessage.Summary]))
        }
      },
      test("lists snapshots after multiple backups of a growing source tree") {
        withResticFixture { fixture =>
          for {
            source <- createRandomSourceTree(
              fixture.workDir.resolve("snapshot-source"),
              maxFolderDepth = 2,
              maxFolderSize = 3,
              numberOfFiles = 4,
              totalSizeBytes = 1024L
            )
            relativeSource = fixture.workDir.relativize(source.root)
            _ <- fixture.service.init(fixture.repo, fixture.commonOptions)
            firstBackup <- fixture.service.backupSummary(
              fixture.repo,
              fixture.commonOptions,
              BackupOptions(tag = List("snapshots-test")),
              basePath = fixture.workDir,
              paths = NonEmptyChunk(relativeSource)
            )
            expandedSource <- createRandomSourceTree(
              source.root,
              maxFolderDepth = 3,
              maxFolderSize = 4,
              numberOfFiles = 6,
              totalSizeBytes = 2048L
            )
            secondBackup <- fixture.service.backupSummary(
              fixture.repo,
              fixture.commonOptions,
              BackupOptions(tag = List("snapshots-test")),
              basePath = fixture.workDir,
              paths = NonEmptyChunk(relativeSource)
            )
            snapshots <- fixture.service.snapshots(
              fixture.repo,
              fixture.commonOptions,
              SnapshotsOptions(tags = Vector("snapshots-test"))
            )
            snapshotIds = snapshots.snapshots.map(_.id).toSet
            snapshotsById = snapshots.snapshots.map(snapshot => snapshot.id -> snapshot).toMap
          } yield assertTrue(snapshotIds == Set(firstBackup.snapshot_id, secondBackup.snapshot_id)) &&
            assertTrue(snapshotsById(firstBackup.snapshot_id).summary.total_files_processed == source.contents.size) &&
            assertTrue(snapshotsById(secondBackup.snapshot_id).summary.total_files_processed == expandedSource.contents.size) &&
            assertTrue(snapshotsById(secondBackup.snapshot_id).summary.total_bytes_processed == 3072L)
        }
      },
      test("forgets old snapshots according to the retention policy") {
        withResticFixture { fixture =>
          for {
            source <- createRandomSourceTree(
              fixture.workDir.resolve("forget-source"),
              maxFolderDepth = 2,
              maxFolderSize = 3,
              numberOfFiles = 4,
              totalSizeBytes = 1024L
            )
            relativeSource = fixture.workDir.relativize(source.root)
            _ <- fixture.service.init(fixture.repo, fixture.commonOptions)
            firstBackup <- fixture.service.backupSummary(
              fixture.repo,
              fixture.commonOptions,
              BackupOptions(tag = List("forget-test")),
              basePath = fixture.workDir,
              paths = NonEmptyChunk(relativeSource)
            )
            _ <- ZIO.attempt(Files.writeString(source.root.resolve("new-file.txt"), "new snapshot contents"))
            secondBackup <- fixture.service.backupSummary(
              fixture.repo,
              fixture.commonOptions,
              BackupOptions(tag = List("forget-test")),
              basePath = fixture.workDir,
              paths = NonEmptyChunk(relativeSource)
            )
            _ <- fixture.service.forget(
              fixture.repo,
              fixture.commonOptions,
              ForgetOptions(keepLast = 1)
            )
            snapshots <- fixture.service.snapshots(
              fixture.repo,
              fixture.commonOptions,
              SnapshotsOptions(tags = Vector("forget-test"))
            )
          } yield assertTrue(
            firstBackup.snapshot_id != secondBackup.snapshot_id,
            snapshots.snapshots.map(_.id).toList == List(secondBackup.snapshot_id)
          )
        }
      },
      suite("password passing forms")(
        passwordForms.map { passwordForm =>
          test(s"runs init, backup, snapshots, and restore with ${passwordForm.name}") {
            withResticFixture { fixture =>
              for {
                credentials <- passwordForm.credentials(fixture)
                result <- runPasswordProtectedLifecycle(fixture, passwordForm.name, credentials)
              } yield result
            }
          }
        } *
      )
    )

  private final case class ResticFixture(
    workDir: Path,
    repo: Repo,
    commonOptions: CommonOptions,
    service: ResticCommandService
  )

  private final case class SourceTree(root: Path, contents: Map[Path, String])

  private final case class PasswordCredentials(commonOptions: CommonOptions, password: Option[String])

  private final case class PasswordForm(
    name: String,
    credentials: ResticFixture => ZIO[Any, Throwable, PasswordCredentials]
  )

  private val resticExecutable: Path =
    Path.of(sys.env.getOrElse("RESTIC_TEST_EXECUTABLE", "/usr/bin/restic"))

  private val testPassword: String =
    "scala-restic-test-password"

  private val passwordForms: Vector[PasswordForm] =
    Vector(
      PasswordForm(
        "no password (--insecure-no-password)",
        fixture => ZIO.succeed(PasswordCredentials(fixture.commonOptions, None))
      ),
      PasswordForm(
        "password command argument (--password-command)",
        _ => ZIO.succeed(PasswordCredentials(
          CommonOptions(noCache = true, passwordCommand = s"printf %s $testPassword"),
          None
        ))
      ),
      PasswordForm(
        "password file (--password-file)",
        fixture =>
          for {
            passwordFile <- ZIO.attempt(fixture.workDir.resolve("restic-password.txt"))
            _ <- ZIO.attempt(Files.writeString(passwordFile, testPassword + java.lang.System.lineSeparator(), StandardCharsets.UTF_8))
          } yield PasswordCredentials(CommonOptions(noCache = true, passwordFile = Some(passwordFile)), None)
      ),
      PasswordForm(
        "stdin password parameter",
        _ => ZIO.succeed(PasswordCredentials(
          CommonOptions(noCache = true),
          Some(testPassword)
        ))
      )
    )

  private def withResticFixture[R, E](test: ResticFixture => ZIO[R, E, TestResult]): ZIO[R, Throwable | E, TestResult] =
    ZIO.scoped {
      for {
        workDir <- tempDirectory("scala-restic-command-service-")
        repoPath = workDir.resolve("repo")
        _ <- ZIO.attempt(Files.createDirectories(repoPath))
        config = ResticConfig(resticExecutable)
        service = ResticCommandServiceImpl(ResticCommandBuilderService(config))
        commonOptions = CommonOptions(insecureNoPassword = true, noCache = true)
        result <- test(ResticFixture(workDir, Repo.atFolder(repoPath), commonOptions, service))
      } yield result
    }

  private def tempDirectory(prefix: String): ZIO[Scope, Nothing, Path] =
    ZIO.acquireRelease(ZIO.attempt(Files.createTempDirectory(prefix)).orDie)(path =>
      ZIO.attempt(deleteRecursively(path)).orDie
    )

  private def runPasswordProtectedLifecycle(
    fixture: ResticFixture,
    passwordFormName: String,
    credentials: PasswordCredentials
  ): ZIO[Any, Throwable, TestResult] =
    for {
      source <- createRandomSourceTree(
        fixture.workDir.resolve(s"password-source-${passwordFormName.replaceAll("[^A-Za-z0-9]+", "-")}"),
        maxFolderDepth = 2,
        maxFolderSize = 3,
        numberOfFiles = 5,
        totalSizeBytes = 2048L
      )
      relativeSource = fixture.workDir.relativize(source.root)
      restoreTarget <- ZIO.attempt(Files.createDirectories(fixture.workDir.resolve("password-restore-target")))
      _ <- fixture.service.init(fixture.repo, credentials.commonOptions, password = credentials.password)
      backupSummary <- fixture.service.backupSummary(
        fixture.repo,
        credentials.commonOptions,
        BackupOptions(tag = List("password-form-test")),
        password = credentials.password,
        basePath = fixture.workDir,
        paths = NonEmptyChunk(relativeSource)
      )
      snapshots <- fixture.service.snapshots(
        fixture.repo,
        credentials.commonOptions,
        SnapshotsOptions(tags = Vector("password-form-test")),
        password = credentials.password
      )
      restoreSummary <- fixture.service.restoreSummary(
        fixture.repo,
        credentials.commonOptions,
        RestoreOptions(target = Some(restoreTarget)),
        password = credentials.password,
        snapshotID = s"${backupSummary.snapshot_id}:$relativeSource"
      )
      restoredContents <- readRelativeFiles(restoreTarget)
      snapshotIds = snapshots.snapshots.map(_.id).toSet
    } yield assert(backupSummary.snapshot_id)(isNonEmptyString) &&
      assertTrue(snapshotIds == Set(backupSummary.snapshot_id)) &&
      assertTrue(snapshots.snapshots.head.summary.total_files_processed == source.contents.size) &&
      assertTrue(restoreSummary.files_restored >= source.contents.size) &&
      assertTrue(restoredContents == source.contents)

  private def createRandomSourceTree(
    root: Path,
    maxFolderDepth: Int,
    maxFolderSize: Int,
    numberOfFiles: Int,
    totalSizeBytes: Long
  ): ZIO[Any, Throwable, SourceTree] =
    for {
      _ <- validateSourceTreeParameters(maxFolderDepth, maxFolderSize, numberOfFiles, totalSizeBytes)
      _ <- ZIO.attempt(Files.createDirectories(root))
      sizes <- distributeFileSizes(numberOfFiles, totalSizeBytes)
      parentFolders <- randomParentFolders(root, numberOfFiles, maxFolderDepth, maxFolderSize)
      _ <- ZIO.foreachDiscard(sizes.zipWithIndex) { case (size, index) =>
        for {
          parent = parentFolders(index / maxFolderSize)
          fileName <- Random.nextUUID.map(uuid => s"file-$index-$uuid.dat")
          content <- randomAsciiContent(size)
          _ <- ZIO.attempt(Files.createDirectories(parent))
          _ <- ZIO.attempt(Files.writeString(parent.resolve(fileName), content, StandardCharsets.UTF_8))
        } yield ()
      }
      contents <- readRelativeFiles(root)
    } yield SourceTree(root, contents)

  private def validateSourceTreeParameters(
    maxFolderDepth: Int,
    maxFolderSize: Int,
    numberOfFiles: Int,
    totalSizeBytes: Long
  ): IO[IllegalArgumentException, Unit] =
    ZIO.fail(IllegalArgumentException("maxFolderDepth must be non-negative")).when(maxFolderDepth < 0).unit *>
      ZIO.fail(IllegalArgumentException("numberOfFiles must be non-negative")).when(numberOfFiles < 0).unit *>
      ZIO.fail(IllegalArgumentException(s"totalSizeBytes must be non-negative: $totalSizeBytes")).when(totalSizeBytes < 0).unit *>
      ZIO.fail(IllegalArgumentException("maxFolderSize must be positive when numberOfFiles is positive"))
        .when(numberOfFiles > 0 && maxFolderSize <= 0).unit *>
      ZIO.fail(IllegalArgumentException("numberOfFiles exceeds maxFolderSize for a zero-depth hierarchy"))
        .when(maxFolderDepth == 0 && numberOfFiles > maxFolderSize).unit

  private def distributeFileSizes(numberOfFiles: Int, totalSizeBytes: Long): UIO[Vector[Int]] =
    if numberOfFiles == 0 then ZIO.succeed(Vector.empty)
    else
      val baseSize = totalSizeBytes / numberOfFiles
      val remainder = (totalSizeBytes % numberOfFiles).toInt
      Random.shuffle((0 until numberOfFiles).toList).map { largerFiles =>
        val largerFileIndexes = largerFiles.take(remainder).toSet
        (0 until numberOfFiles).map { index =>
          (baseSize + (if largerFileIndexes.contains(index) then 1L else 0L)).toInt
        }.toVector
      }

  private def randomParentFolders(
    root: Path,
    numberOfFiles: Int,
    maxFolderDepth: Int,
    maxFolderSize: Int
  ): UIO[Vector[Path]] =
    if numberOfFiles == 0 then ZIO.succeed(Vector.empty)
    else
      val numberOfFolders = ((numberOfFiles - 1) / maxFolderSize) + 1
      ZIO.foreach(0 until numberOfFolders)(folderGroup => randomParentFolder(root, folderGroup, maxFolderDepth)).map(_.toVector)

  private def randomParentFolder(root: Path, folderGroup: Int, maxFolderDepth: Int): UIO[Path] =
    if maxFolderDepth == 0 then ZIO.succeed(root)
    else
      for {
        folderDepth <- Random.nextIntBetween(1, maxFolderDepth + 1)
        folderNames <- ZIO.foreach(0 until folderDepth) { depth =>
          Random.nextUUID.map(uuid => s"group-$folderGroup-level-$depth-$uuid")
        }
      } yield folderNames.foldLeft(root)((path, folderName) => path.resolve(folderName))

  private def randomAsciiContent(sizeBytes: Int): UIO[String] =
    Random.nextString(sizeBytes).map(_.map { char =>
      val printableAsciiStart = 32
      val printableAsciiRange = 95
      ((char.toInt.abs % printableAsciiRange) + printableAsciiStart).toChar
    })

  private def readRelativeFiles(root: Path): ZIO[Any, Throwable, Map[Path, String]] =
    ZIO.attempt {
      val stream = Files.walk(root)
      try
        stream
          .iterator()
          .asScala
          .filter(Files.isRegularFile(_))
          .map(path => root.relativize(path) -> Files.readString(path, StandardCharsets.UTF_8))
          .toMap
      finally stream.close()
    }

  private def logResticMessage(command: String)(message: BackupMessage | RestoreMessage): UIO[Unit] =
    ZIO.logDebug(s"Decoded restic $command message: $message")

  private def deleteRecursively(path: Path): Unit =
    if Files.exists(path) then
      val stream = Files.walk(path)
      try
        stream
          .iterator()
          .asScala
          .toVector
          .sortBy(_.toString.length)
          .reverse
          .foreach(Files.deleteIfExists)
      finally stream.close()
