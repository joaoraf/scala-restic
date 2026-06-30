package br.gov.lexml.scala_restic.command.impl

import br.gov.lexml.scala_restic.command.{ResticCommandBuilderService, ResticCommandService, ResticException}
import br.gov.lexml.scala_restic.data.backup.BackupMessage
import br.gov.lexml.scala_restic.data.common.ResticExitCode.{REC_SUCCESS, asRestic}
import br.gov.lexml.scala_restic.data.init.InitResult
import br.gov.lexml.scala_restic.data.restore.RestoreMessage
import br.gov.lexml.scala_restic.data.snapshots.Snapshots
import br.gov.lexml.scala_restic.options.backup.BackupOptions
import br.gov.lexml.scala_restic.options.common.{CommonOptions, Repo}
import br.gov.lexml.scala_restic.options.init.InitOptions
import br.gov.lexml.scala_restic.options.restore.RestoreOptions
import br.gov.lexml.scala_restic.options.snapshots.SnapshotsOptions
import zio.*
import zio.stream.*

import java.nio.file.Path

final class ResticCommandServiceImpl(rb: ResticCommandBuilderService) extends ResticCommandService:
  private def passwordStream(password: Option[String]): ZStream[Any, Exception, String] =
    password.fold(ZStream.empty)(value => ZStream(value + java.lang.System.lineSeparator()))

  override def checkRepositoryExistence(
    repo: Repo,
    commonOptions: CommonOptions,
    password: Option[String]
  ): IO[Exception, Boolean] =
    rb.commandBuilder(repo)
      .options(commonOptions)
      .stdinStringStream(passwordStream(password))
      .command("cat", "config")
      .exitCode
      .map(_.asRestic)
      .map(_ == REC_SUCCESS)

  override def init(
    repo: Repo,
    commonOptions: CommonOptions,
    initOptions: InitOptions,
    password: Option[String]
  ): IO[Exception, InitResult] =
    for {
      process <- rb.commandBuilder(repo)
        .options(commonOptions.withJson)
        .stdinStringStream(passwordStream(password))
        .options(initOptions)
        .command("init")
        .run
      stdoutFiber <- process.stdout.string.flatMap { result =>
        ZIO.fromEither(InitResult.jsonCodec.decodeJson(result))
          .mapError(error => ResticException(s"Error decoding init result: $error"))
      }.fork
      exitCode <- process.exitCode.map(_.asRestic)
      _ <- ZIO.unless(exitCode == REC_SUCCESS) {
        ZIO.fail(ResticException("Error initializing repository", Some(exitCode)))
      }
      result <- stdoutFiber.join
    } yield result

  override def backupStream(
    repo: Repo,
    commonOptions: CommonOptions,
    backupOptions: BackupOptions,
    password: Option[String],
    basePath: Path,
    paths: NonEmptyChunk[Path]
  ): IO[Exception, ZStream[Any, Exception, BackupMessage]] =
    for {
      process <- rb.commandBuilder(repo)
        .options(commonOptions.withJson)
        .options(backupOptions)
        .workingDirectory(basePath.toFile)
        .args(paths.map(_.toString) *)
        .stdinStringStream(passwordStream(password))
        .command("backup")
        .redirectErrorStream(true)
        .run
      outStream = process.stdout.linesStream.via(BackupMessage.jsonDecoderPipeline)
      streamTail = ZStream.unwrap {
        process.exitCode.map(_.asRestic).flatMap {
          case REC_SUCCESS => ZIO.succeed(ZStream.empty)
          case exitCode => ZIO.fail(ResticException("Error in backup process", exitCode = Some(exitCode)))
        }
      }
    } yield outStream ++ streamTail

  override def backupSummary(
    repo: Repo,
    commonOptions: CommonOptions,
    backupOptions: BackupOptions,
    password: Option[String],
    basePath: Path,
    paths: NonEmptyChunk[Path]
  ): IO[Exception, BackupMessage.Summary] =
    for {
      stream <- backupStream(repo, commonOptions, backupOptions, password, basePath, paths)
      result <- stream.runFold[Option[BackupMessage.Summary]](None) {
        case (_, summary: BackupMessage.Summary) => Some(summary)
        case (current, _) => current
      }.some.mapError(error => ResticException(s"Error getting backup summary: $error"))
    } yield result

  override def restoreStream(
    repo: Repo,
    commonOptions: CommonOptions,
    restoreOptions: RestoreOptions,
    password: Option[String],
    snapshotID: String
  ): IO[Exception, ZStream[Any, Exception, RestoreMessage]] =
    for {
      process <- rb.commandBuilder(repo)
        .options(commonOptions.withJson)
        .options(restoreOptions)
        .stdinStringStream(passwordStream(password))
        .args(snapshotID)
        .command("restore")
        .redirectErrorStream(true)
        .run
      outStream = process.stdout.linesStream.via(RestoreMessage.jsonDecoderPipeline)
      streamTail = ZStream.unwrap {
        process.exitCode.map(_.asRestic).flatMap {
          case REC_SUCCESS => ZIO.succeed(ZStream.empty)
          case exitCode => ZIO.fail(ResticException("Error in restore process", exitCode = Some(exitCode)))
        }
      }
    } yield outStream ++ streamTail

  override def restoreSummary(
    repo: Repo,
    commonOptions: CommonOptions,
    restoreOptions: RestoreOptions,
    password: Option[String],
    snapshotID: String
  ): IO[Exception, RestoreMessage.Summary] =
    for {
      stream <- restoreStream(repo, commonOptions, restoreOptions, password, snapshotID)
      result <- stream.runFold[Option[RestoreMessage.Summary]](None) {
        case (_, summary: RestoreMessage.Summary) => Some(summary)
        case (current, _) => current
      }.some.mapError(error => ResticException(s"Error getting restore summary: $error"))
    } yield result

  override def snapshots(
    repo: Repo,
    commonOptions: CommonOptions,
    snapshotsOptions: SnapshotsOptions,
    password: Option[String]
  ): IO[Exception, Snapshots] =
    for {
      process <- rb.commandBuilder(repo)
        .options(commonOptions.withJson)
        .options(snapshotsOptions)
        .stdinStringStream(passwordStream(password))
        .command("snapshots")
        .redirectErrorStream(true)
        .run
      result <- process.stdout.string
      exitCode <- process.exitCode.map(_.asRestic)
      _ <- ZIO.fail(ResticException("Error getting snapshots", Some(exitCode))).when(exitCode != REC_SUCCESS)
      snapshots <- ZIO.fromEither(Snapshots.jsonCodec.decodeJson(result))
        .mapError(message => ResticException(s"Error decoding snapshots output: $message"))
    } yield snapshots

object ResticCommandServiceImpl:
  val layer: URLayer[ResticCommandBuilderService, ResticCommandService] =
    ZLayer.fromFunction(ResticCommandServiceImpl.apply)
