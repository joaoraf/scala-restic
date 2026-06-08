package br.gov.lexml.scala_restic.command

import br.gov.lexml.scala_restic.config.ResticConfig
import br.gov.lexml.scala_restic.data.common.ResticExitCode
import br.gov.lexml.scala_restic.data.common.ResticExitCode.{REC_INEXISTENT_REPO, REC_SUCCESS}
import br.gov.lexml.scala_restic.options.common.CommonOptions
import zio.*
import zio.stream.*
import zio.process.*

import java.io.File
import java.nio.file.Path

final case class ResticException(str: String, exitCode: Some[ResticExitCode] = None)
  extends Exception(exitCode.fold(str)(ec => s"$str (exit code: $ec)"))

final case class ResticCommand(
  workingDirectory : Option[File] = None,
  stdin : ZStream[Any, CommandError, Byte] = ZStream.empty,
  stdinFlushChunksEagerly: Boolean = false,
  redirectErrorStream : Boolean = false
):
  def withWorkingDirectory(wd : File) = copy(workingDirectory = Some(wd))
  def withStdinLinesUTF8(lines : String*) =
    copy(stdin = ZStream(lines*).via(ZPipeline.utf8Encode).mapError(CommandError.IOError))
  private def command(opts : String*) =
    ZIO.config(ResticConfig.config)
      .map { cfg =>
        Command.Standard(
          command = NonEmptyChunk.apply(cfg.resticExecutablePath, opts*),
          env = Map(),
          workingDirectory = workingDirectory,
          stdin = ProcessInput.fromStream(stdin),
          stdout = ProcessOutput.Pipe,
          stderr = ProcessOutput.Pipe,
          redirectErrorStream = redirectErrorStream
        )
      }


object ResticCommands:
  final case class ResticProcessEnv(
    workingDirectory : Option[Path] = None,
    stdin : Option[ZStream[Any,Exception,Byte]] = None
  )
  private def resticCommand(opts : String*) =
    ZIO.config(ResticConfig.config)
      .map { cfg => Command(cfg.resticExecutablePath, opts*) }



  def checkRepositoryExistence(opts : CommonOptions = CommonOptions(), password : Option[String] = None) : ZIO[Any,Exception,Boolean] =
    for {
      cmd <- resticCommand("cat","config")
      process <- cmd.run
      _ <- process.
      ec <- cmd.exitCode.map(ResticExitCode(_))
      _ <- ZIO.when(ec != REC_SUCCESS && ec != REC_INEXISTENT_REPO) {
        ZIO.fail(ResticException("Error checking resitory existence with 'cat config'", Some(ec)))
      }
    } yield (ec == REC_SUCCESS)





