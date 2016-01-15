package com.cloudera.spark.bulkload.serializable

import java.io.{ObjectInputStream, ObjectOutputStream}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Serializer, Kryo, KryoSerializable}
import org.apache.hadoop.io.WritableComparator

/**
  * Created by root on 1/8/16.
  */
class BytesSerializer(input: Array[Byte]) extends Serializer[BytesSerializable] {
  override def write(kryo: Kryo, output: Output, t: BytesSerializable): Unit = {
    kryo.writeObject(output, t.getBytes())
  }

  override def read(kryo: Kryo, input: Input, aClass: Class[BytesSerializable]): BytesSerializable = {
    return new BytesSerializable(kryo.readObject(input, classOf[Array[Byte]]))
  }

}
