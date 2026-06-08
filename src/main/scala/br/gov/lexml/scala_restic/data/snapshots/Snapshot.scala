package br.gov.lexml.scala_restic.data.snapshots

import zio.*
import zio.schema.codec.JsonCodec
import zio.schema.{DeriveSchema, Schema}

import java.time.ZonedDateTime
import zio.json.{JsonCodec as ZJsonCodec}

final case class Snapshots(snapshots : Vector[Snapshot])

object Snapshots:
    given schema : Schema[Snapshots] = DeriveSchema.gen[Snapshots]
    val jsonCodec : ZJsonCodec[Snapshots] =
        ZJsonCodec.vector[Snapshot](using Snapshot.jsonCodec.encoder, Snapshot.jsonCodec.decoder)
          .transform(Snapshots.apply, _.snapshots)

final case class Snapshot(
    time : ZonedDateTime,
    tree : String,
    paths : Vector[String],
    hostname : String,
    username : String,
    uid : Int,
    gid : Int,
    program_version : String,
    summary : SnapshotSummary,
    id : String,
    short_id : String
    )

object Snapshot:
    given schema : Schema[Snapshot] = DeriveSchema.gen[Snapshot]
    val jsonCodec : ZJsonCodec[Snapshot] = JsonCodec.jsonCodec(schema)

final case class SnapshotSummary(
    backup_start : ZonedDateTime,
    backup_end : ZonedDateTime,
    files_new : Long,
    files_changed : Long,
    files_unmodified : Long,
    dirs_new : Long,
    dirs_changed : Long,
    dirs_unmodified : Long,
    data_blobs : Long,
    tree_blobs : Long,
    data_added : Long,
    data_added_packed : Long,
    total_files_processed : Long,
    total_bytes_processed : Long
    )

object SnapshotSummary:
    given schema : Schema[SnapshotSummary] = DeriveSchema.gen[SnapshotSummary]
    val jsonCodec : ZJsonCodec[SnapshotSummary] = JsonCodec.jsonCodec(schema)