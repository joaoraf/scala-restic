package br.gov.lexml.scala_restic.data.restore

import zio.*
import zio.stream.*
import zio.schema.*
import zio.schema.annotation.*
import br.gov.lexml.scala_restic.data.common.CommonSchemas.given
import zio.json.JsonCodec as ZJsonCodec
import zio.schema.NameFormat.SnakeCase
import zio.schema.codec.JsonCodec

import java.nio.file.Path
import java.time.ZonedDateTime

enum VerboseStatusAction:
  @caseName("restored") case Restored
  @caseName("updated") case Updated
  @caseName("unchanged") case Unchanged
  @caseName("deleted") case Deleted

@discriminatorName("message_type")
enum RestoreMessage:
  @caseName("status")
  case Status(
             seconds_elapsed : Long = 0L,
             percent_done : Double = 0.0,
             total_files : Long = 0L,
             files_restored : Long = 0L,
             files_skipped : Long = 0L,
             files_deleted : Long = 0L,
             total_bytes : Long = 0L,
             bytes_restored : Long = 0L,
             bytes_skipped : Long = 0L
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
                    size : Long = 0L
                    )
  
  @caseName("summary")
  case Summary(
      seconds_elapsed : Long = 0L,
      total_files : Long = 0L,        
      files_restored : Long = 0L,
      files_skipped : Long = 0L,
      files_deleted : Long = 0L,
      total_bytes : Long = 0L,
      bytes_restored : Long = 0L,
      bytes_skipped : Long = 0L
      )
  
  
