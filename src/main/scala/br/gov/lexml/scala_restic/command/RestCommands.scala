package br.gov.lexml.scala_restic.command

import br.gov.lexml.scala_restic.config.ResticConfig
import br.gov.lexml.scala_restic.data.backup.BackupMessage
import br.gov.lexml.scala_restic.data.common.ResticExitCode
import br.gov.lexml.scala_restic.data.common.ResticExitCode.{REC_INEXISTENT_REPO, REC_SUCCESS, asRestic}
import br.gov.lexml.scala_restic.data.init.InitResult
import br.gov.lexml.scala_restic.options.ResticOptionSource
import br.gov.lexml.scala_restic.options.backup.BackupOptions
import br.gov.lexml.scala_restic.options.common.{CommonOptions, Repo}
import br.gov.lexml.scala_restic.options.init.InitOptions
import zio.*
import zio.stream.*
import zio.process.*

import java.io.{File, IOException}
import java.nio.file.Path

final case class ResticException(str: String, exitCode: Option[ResticExitCode] = None,cause : Throwable = null)
  extends Exception(exitCode.fold(str)(ec => s"$str (exit code: $ec)"),cause)

final case class ResticCommandBuilder(
  config : ResticConfig,
  repo : Repo,
  workingDirectory : Option[File] = None,
  stdin : ZStream[Any, Exception, Byte] = ZStream.empty,
  stdinFlushChunksEagerly: Boolean = false,
  redirectErrorStream : Boolean = false,
  args : Chunk[String] = Chunk.empty
):
  def workingDirectory(wd : File): ResticCommandBuilder = copy(workingDirectory = Some(wd))
  def stdinLinesUTF8(lines : String*): ResticCommandBuilder =
    stdinStringStream(ZStream(lines.map(_ + System.lineSeparator)*))
  def stdinByteStream(stream : ZStream[Any,Exception,Byte]): ResticCommandBuilder = copy(stdin = stream)
  def stdinStringStream(stream : ZStream[Any,Exception,String]): ResticCommandBuilder =
    copy(stdin = stream.via(ZPipeline.utf8Encode))
  def args(additionalArgs: String*): ResticCommandBuilder = copy(args = args ++ additionalArgs)
  def options(opts : ResticOptionSource) : ResticCommandBuilder = copy(args = args ++ opts.toArgs)
  def command(opts : String*): Command = {
    val stdin1 = stdin.mapError {
      case e : IOException => CommandError.IOError(e)
      case t : Throwable => CommandError.Error(t)
    }
    Command.Standard(
      command = NonEmptyChunk.apply(config.resticExecutablePath.toString, (repo.toArgs ++ args ++ opts)*),
      env = Map(),
      workingDirectory = workingDirectory,
      stdin = ProcessInput.fromStream(stdin1),
      stdout = ProcessOutput.Pipe,
      stderr = ProcessOutput.Pipe,
      redirectErrorStream = redirectErrorStream
    )
  }

final class ResticCommandBuilderService(config : ResticConfig):
  def commandBuilder(repo : Repo) : ResticCommandBuilder = ResticCommandBuilder(config,repo)

object ResticCommandBuilderService:
  val layer: ZLayer[Any, Config.Error, ResticCommandBuilderService] =
    ZLayer.fromZIO(ZIO.config(ResticConfig.config).map(ResticCommandBuilderService(_)))


final class ResticCommandService(rb : ResticCommandBuilderService):

  def checkRepositoryExistence(repo : Repo, commonOptions : CommonOptions = CommonOptions(), password : Option[String] = None) : ZIO[Any,Exception,Boolean] =
    for {
      ec <- rb.commandBuilder(repo).options(commonOptions).command("cat","config").exitCode.map(_.asRestic)
      _ <- ZIO.when(ec != REC_SUCCESS && ec != REC_INEXISTENT_REPO) {
        ZIO.fail(ResticException("Error checking resitory existence with 'cat config'", Some(ec)))
      }
    } yield (ec == REC_SUCCESS)

  def init(
    repo : Repo,
    commonOptions : CommonOptions = CommonOptions(),
    initOptions : InitOptions = InitOptions(),
    password : Option[String] = None
  ) : ZIO[Any,Exception,InitResult] =
    for {
      process <- rb.commandBuilder(repo)
        .options(commonOptions.withJson)
        .options(initOptions)
        .command("init")
        .run
      stdoutFiber <- process.stdout.string.flatMap {
          res => ZIO.fromEither(InitResult.jsonCodec.decodeJson(res))
            .mapError(e => ResticException(s"Error decoding init result: $e"))
        }.fork
      ec <- process.exitCode.map(_.asRestic)
      _ <- ZIO.unless(ec == REC_SUCCESS) { ZIO.fail(ResticException("Error initializing repository", Some(ec))) }
      res <- stdoutFiber.join
    } yield res

  def backupStream(
    repo : Repo,
    commonOptions: CommonOptions = CommonOptions(),
    backupOptions: BackupOptions = BackupOptions(),
    password: Option[String] = None,
    paths: NonEmptyChunk[Path]
  ): IO[Exception, ZStream[Any, Exception, BackupMessage]] =
    for {
      process <- rb.commandBuilder(repo)
        .options(commonOptions.withJson)
        .options(backupOptions)
        .args(paths.map(_.toString) *)
        .command("backup")
        .redirectErrorStream(true)
        .run

      summaryPromise <- Promise.make[Nothing, BackupMessage.Summary]
      outStream =
        process.stdout.linesStream.via(BackupMessage.jsonDecoderPipeline)
      streamTail = ZStream.unwrap {
        process.exitCode.map(_.asRestic).flatMap {
          case REC_SUCCESS => ZIO.succeed(ZStream.empty)
          case ec => ZIO.fail(ResticException("Error in backup process", exitCode = Some(ec)))
        }
      }
    } yield outStream ++ streamTail
  end backupStream

  def backupSummary( repo : Repo,
    commonOptions: CommonOptions = CommonOptions(),
    backupOptions: BackupOptions = BackupOptions(),
    password: Option[String] = None,
    paths: NonEmptyChunk[Path]
  ): IO[Exception, BackupMessage.Summary] =
    for {
      stream <- backupStream(repo, commonOptions, backupOptions, password, paths)
      res <- stream.runFold[Option[BackupMessage.Summary]](None) {
        case (_,m : BackupMessage.Summary) => Some(m)
        case (x,_) => x
      }.some.mapError(e => ResticException(s"Error getting backup summary: $e"))
    } yield res





