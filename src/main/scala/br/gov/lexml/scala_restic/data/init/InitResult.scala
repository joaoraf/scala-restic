package br.gov.lexml.scala_restic.data.init

import java.net.URL
import zio.*
import zio.schema.codec.JsonCodec
import zio.schema.{DeriveSchema, Schema}
import zio.json.JsonCodec as ZJsonCodec


final case class InitResult(
  id : String,
  repository : URL
)

object InitResult:
  given schema: Schema[InitResult] = DeriveSchema.gen[InitResult]
  val jsonCodec: ZJsonCodec[InitResult] = JsonCodec.jsonCodec(schema)

