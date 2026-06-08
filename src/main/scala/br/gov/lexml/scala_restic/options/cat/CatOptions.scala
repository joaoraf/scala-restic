package br.gov.lexml.scala_restic.options.cat

import java.nio.file.Path

enum CatOptions:
  case MasterKey, Config
  case Pack(id : String)
  case Blob(id : String)
  case Snapshot(id : String)
  case Index(id : String)
  case Key(id : String)
  case Lock(id : String)
  case Tree(snapshot : String, subfolder : Path)

