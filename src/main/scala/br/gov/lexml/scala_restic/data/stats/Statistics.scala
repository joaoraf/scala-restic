package br.gov.lexml.scala_restic.data.stats

import zio.*
import zio.schema.codec.JsonCodec
import zio.schema.{DeriveSchema, Schema}

import zio.json.{JsonCodec as ZJsonCodec}


final case class Statistics(
  total_size : Long,
  total_file_count : Long,
  snapshots_count : Long
)

object Statistics:
  given schema : Schema[Statistics] = DeriveSchema.gen[Statistics]
  val jsonCodec : ZJsonCodec[Statistics] = JsonCodec.jsonCodec(schema)
