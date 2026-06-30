package br.gov.lexml.scala_restic.command

import br.gov.lexml.scala_restic.config.ResticConfig
import br.gov.lexml.scala_restic.data.common.ResticExitCode
import br.gov.lexml.scala_restic.options.ResticOptionSource
import br.gov.lexml.scala_restic.options.common.Repo
import zio.*
import zio.stream.*
import zio.process.*

import java.io.{File, IOException}

final case class ResticException(str: String, exitCode: Option[ResticExitCode] = None, cause: Throwable = null)
  extends Exception(exitCode.fold(str)(ec => s"$str (exit code: $ec)"), cause)

final case class ResticCommandBuilder(
  config: ResticConfig,
  repo: Repo,
  workingDirectory: Option[File] = None,
  stdin: ZStream[Any, Exception, Byte] = ZStream.empty,
  stdinFlushChunksEagerly: Boolean = false,
  redirectErrorStream: Boolean = false,
  args: Chunk[String] = Chunk.empty
):
  def workingDirectory(wd: File): ResticCommandBuilder = copy(workingDirectory = Some(wd))

  def stdinLinesUTF8(lines: String*): ResticCommandBuilder =
    stdinStringStream(ZStream(lines.map(_ + System.lineSeparator) *))

  def stdinByteStream(stream: ZStream[Any, Exception, Byte]): ResticCommandBuilder = copy(stdin = stream)

  def stdinStringStream(stream: ZStream[Any, Exception, String]): ResticCommandBuilder =
    copy(stdin = stream.via(ZPipeline.utf8Encode))

  def args(additionalArgs: String*): ResticCommandBuilder = copy(args = args ++ additionalArgs)

  def options(opts: ResticOptionSource): ResticCommandBuilder = copy(args = args ++ opts.toArgs)

  def command(opts: String*): Command = {
    val stdin1 = stdin.mapError {
      case e: IOException => CommandError.IOError(e)
      case t: Throwable => CommandError.Error(t)
    }
    Command.Standard(
      command = NonEmptyChunk.apply(config.resticExecutablePath.toString, repo.toArgs ++ opts ++ args *),
      env = Map(),
      workingDirectory = workingDirectory,
      stdin = ProcessInput.fromStream(stdin1),
      stdout = ProcessOutput.Pipe,
      stderr = ProcessOutput.Pipe,
      redirectErrorStream = redirectErrorStream
    )
  }

final class ResticCommandBuilderService(config: ResticConfig):
  def commandBuilder(repo: Repo): ResticCommandBuilder = ResticCommandBuilder(config, repo)

object ResticCommandBuilderService:
  val layer: ZLayer[Any, Config.Error, ResticCommandBuilderService] =
    ZLayer.fromZIO(ZIO.config(ResticConfig.config).map(ResticCommandBuilderService(_)))
