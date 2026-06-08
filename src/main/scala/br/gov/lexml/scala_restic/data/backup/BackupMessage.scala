package br.gov.lexml.scala_restic.data.backup

import zio.*
import zio.stream.*
import zio.schema.*
import zio.schema.annotation.*
import br.gov.lexml.scala_restic.data.common.CommonSchemas.given
import zio.json.{JsonCodec as ZJsonCodec}
import zio.schema.NameFormat.SnakeCase
import zio.schema.codec.JsonCodec

import java.nio.file.Path
import java.time.ZonedDateTime

@discriminatorName("message_type")
enum BackupMessage:
  @caseName("status")
  case Status(
             seconds_elapsed : Long = 0L,
             seconds_remaining : Long = 0L,
             percent_done : Double = 0.0,
             total_files : Long = 0L,
             files_done : Long = 0L,
             total_bytes : Long = 0L,
             bytes_done : Long = 0L,
             error_count : Long = 0L,
             current_files : Vector[Path] = Vector()
             )
  @caseName("error")
  case Error(
            @fieldName("error.message") message : String,
            during : String,
            item : String
            )
  @caseName("verbose_status")
  case VerboseStatus(
                    action : VerboseStatusAction,
                    item : String,
                    duration : Double = 0.0,
                    data_size : Long = 0L,
                    data_size_in_repo : Long = 0L,
                    metadata_size : Long = 0L,
                    metadata_size_in_repo : Long = 0L,
                    total_files : Long = 0L
                    )
  @caseName("summary")
  case Summary(
      dry_run : Boolean = false,
      files_new : Long = 0L,
      files_changed : Long = 0L,
      files_unmodified : Long = 0L,
      dirs_new : Long = 0L,
      dirs_changed : Long = 0L,
      dirs_unmodified : Long = 0L,
      data_blobs : Long = 0L,
      tree_blobs : Long = 0L,
      data_added : Long = 0L,
      total_files_processed : Long = 0L,
      total_bytes_processed : Long = 0L,
      backup_start : ZonedDateTime,
      backup_end : ZonedDateTime,
      total_duration : Double = 0.0,
      snapshot_id : String
  )
object BackupMessage:
  given schema: Schema[BackupMessage] = DeriveSchema.gen[BackupMessage]
  given jsonCodec: ZJsonCodec[BackupMessage] = JsonCodec.jsonCodec(schema)

  val jsonDecoderPipeline : ZPipeline[Any,Exception,String,BackupMessage] =
    ZPipeline.mapEitherChunked(jsonCodec.decodeJson)
      .mapError(e => new Exception(s"Error decoding backup message: $e"))

enum VerboseStatusAction:
  @caseName("new") case New
  @caseName("unchanged") case Unchanged
  @caseName("modified") case Modified
  @caseName("scan_finished") case ScanFinished

object VerboseStatusAction:
  given schema: Schema[VerboseStatusAction] = DeriveSchema.gen[VerboseStatusAction]

