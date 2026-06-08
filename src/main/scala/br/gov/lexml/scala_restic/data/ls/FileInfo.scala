package br.gov.lexml.scala_restic.data.ls

import br.gov.lexml.scala_restic.data.snapshots.{Snapshot, Snapshots}

import java.nio.file.{Path, Paths}
import java.time.ZonedDateTime
import zio.*
import zio.stream.*
import zio.schema.codec.JsonCodec
import zio.schema.annotation.caseName
import zio.schema.{DeriveSchema, Schema}
import zio.json.JsonCodec as ZJsonCodec
import br.gov.lexml.scala_restic.data.common.CommonSchemas.given

enum EntryType:
  @caseName("file") case  File
  @caseName("dir") case Directory

object EntryType:
  given schema: Schema[EntryType] = DeriveSchema.gen[EntryType]
  given jsonCodec: ZJsonCodec[EntryType] = ZJsonCodec.string.transformOrFail(_ match {
    case "file" => Right(EntryType.File)
    case "dir" => Right(EntryType.Directory)
    case x =>
      Left(s"Invalid entry type: '$x'")
  }, _ match {
    case EntryType.File => "file"
    case EntryType.Directory => "dir"
  })


final case class EntryInfo(
  name : String,
  `type` : EntryType,
  path : Path,
  uid : Int,
  gid : Int,
  mode : Long,
  permissions: String,
  mtime : ZonedDateTime,
  atime : ZonedDateTime,
  ctime : ZonedDateTime,
  inode : Long,
  message_type : String,
  struct_type : String
)

object EntryInfo:
  given schema: Schema[EntryInfo] = DeriveSchema.gen[EntryInfo]
  val jsonCodec: ZJsonCodec[EntryInfo] = JsonCodec.jsonCodec(schema)

final case class LsStream[E](
                         snapshot : Snapshot,
                         entries : ZStream[Any,E | Exception,EntryInfo]
                         )
object LsStream:
  def decodeLsJsonOutput[E](inputStream : ZStream[Any,E,String]) : ZIO[Any,Exception,LsStream[E]] =
    for {
      snapshot <- inputStream.take(1).runCollect.map(_.head).map(Snapshot.jsonCodec.decodeJson).right.mapError(e => new Exception(s"Error decoding snapshot: $e"))
      eiStream = inputStream.drop(1).mapZIO { eiTxt =>
        ZIO.fromEither(EntryInfo.jsonCodec.decodeJson(eiTxt))
          .mapError(e => new Exception(s"Error decoding entry info: $e"))
      }
    } yield (LsStream(snapshot,eiStream))