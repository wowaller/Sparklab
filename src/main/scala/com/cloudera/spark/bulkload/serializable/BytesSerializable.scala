package com.cloudera.spark.bulkload.serializable

import java.io.{DataInputStream, DataOutputStream, ObjectInputStream, ObjectOutputStream}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.io.{WritableComparator, BytesWritable}

/**
  * Created by root on 1/8/16.
  */
class BytesSerializable(input: Array[Byte]) extends Comparable[BytesSerializable] with Serializable with KryoSerializable {

  var bytes = new Array[Byte](input.length)
  Array.copy(input, 0, this.bytes, 0, input.length)

  val serialVersionUID = -1322322139926390329L

  def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    out.writeObject(bytes)
  }

  def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    bytes = in.readObject().asInstanceOf[Array[Byte]]
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    kryo.writeObject(output, bytes)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    bytes = kryo.readObject[Array[Byte]](input, classOf[Array[Byte]])
  }

  def readObjectNoData(): Unit = {

  }

  def setBytes(input: Array[Byte]): Unit = {
    this.bytes = new Array[Byte](input.length)
    Array.copy(input, 0, this.bytes, 0, input.length)
  }

  def getBytes(): Array[Byte] = {
    return bytes
  }

  override def compareTo(o: BytesSerializable): Int = {
    return WritableComparator.compareBytes(bytes, 0, bytes.length,o.bytes, 0, o.bytes.length)
  }

  override def equals(o: Any): Boolean = {
    if (o.isInstanceOf[BytesSerializable]) {
      return compareTo(o.asInstanceOf[BytesSerializable]) == 0
    }
    false
  }

  override def hashCode(): Int = {
    WritableComparator.hashBytes(bytes, bytes.length);
  }

}
