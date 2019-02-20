package com.skt.skon.taxi.schemas

import java.nio.charset.StandardCharsets

import com.skt.skon.taxi.TaxiDistance
import org.apache.flink.api.common.serialization.SerializationSchema
import org.json4s.DefaultFormats
import org.json4s.native.Serialization.write

class TaxiSchema[T] extends SerializationSchema[T] {

  override def serialize(element: T): Array[Byte] = {
    implicit val formats : DefaultFormats.type = DefaultFormats
    write(element).getBytes(StandardCharsets.UTF_8)
  }
}
