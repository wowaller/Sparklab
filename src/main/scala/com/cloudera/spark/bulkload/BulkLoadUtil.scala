package com.cloudera.spark.bulkload

import java.io.IOException
import java.net.URLEncoder
import java.util

import com.cloudera.spark.bulkload.serializable.{CompactBuffer, BytesSerializer, BytesSerializable}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{HTable, Put, RegionLocator, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.mapreduce._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.{InputFormat, MRJobConfig}
import org.apache.spark.rdd.{ShuffledRDD, RDD}
import org.apache.spark.{Aggregator, Logging, Partitioner, SparkContext}

import scala.collection.mutable
import scala.reflect.ClassTag

/**
  * Created by root on 10/13/15.
  */
object BulkLoadUtil extends Logging {
  val BLOCK_SIZE_FAMILIES_CONF_KEY: String = "hbase.mapreduce.hfileoutputformat.blocksize"
  val BLOOM_TYPE_FAMILIES_CONF_KEY: String = "hbase.hfileoutputformat.families.bloomtype"
  val COMPRESSION_FAMILIES_CONF_KEY: String = "hbase.hfileoutputformat.families.compression"
  val DATABLOCK_ENCODING_FAMILIES_CONF_KEY: String = "hbase.mapreduce.hfileoutputformat.families.datablock.encoding"

  val OUTPUT_KEY_CLASS = classOf[ImmutableBytesWritable]
  val OUTPUT_VALUE_CLASS = classOf[Cell]

  def doBulkload[K <: Writable, V <: Writable, F <: InputFormat[K, V]]
  (sc: SparkContext, conf: Configuration,
   inputPath: String, parser: Parser[(K, V), Put],
   table: String, hfilePath: String)
  (implicit kct: ClassTag[K], vct: ClassTag[V], fct: ClassTag[F]) = {
    val input = setupInput(sc, conf, inputPath, parser)(kct, vct, fct)
    writeHFile(sc, conf, input, hfilePath, new HTable(conf, table))
  }

  def setupInput[K <: Writable, V <: Writable, F <: InputFormat[K, V]]
  (sc: SparkContext, conf: Configuration,
   path: String, parser: Parser[(K, V), Put])
  (implicit kct: ClassTag[K], vct: ClassTag[V], fct: ClassTag[F]): RDD[Put] = {
      sc.newAPIHadoopFile(path)(kct, vct, fct).map { case (k: K, v: V) => {
        //      (k.toString + v.toString)
        parser.parse((k, v))
      }
    }

  }

  def writeHFile(sc: SparkContext, conf: Configuration,
                 puts: RDD[Put], path: String, table: HTable): Unit = {

    configureIncrementalLoad(conf, table, table, path)

    val p: Partitioner = new SplitPartitioner(getRegionStartKeys(table))

    implicit val BytesSerializableOrding = new Ordering[BytesSerializable] {
      override def compare(a: BytesSerializable, b: BytesSerializable) = {
        val aBytes = a.getBytes()
        val bBytes = b.getBytes()
        WritableComparator.compareBytes(aBytes, 0, aBytes.length, bBytes, 0, bBytes.length)
      }
    }

    val createCombiner = (v: Array[Byte]) => CompactBuffer[Array[Byte]](v)
    val mergeValue = (buf: CompactBuffer[Array[Byte]], v: Array[Byte]) => buf += v
    val mergeCombiners = (c1: CompactBuffer[Array[Byte]], c2: CompactBuffer[Array[Byte]]) => c1 ++= c2
    val aggregator = new Aggregator[BytesSerializable, Array[Byte], CompactBuffer[Array[Byte]]](
      createCombiner,
      mergeValue,
      mergeCombiners)

    val shuffledPuts = new ShuffledRDD[BytesSerializable, Array[Byte], CompactBuffer[Array[Byte]]](
      puts.flatMap(put => putToBytes(put).map{
        case (key, value) => (new BytesSerializable(key), value)
      }), p)
      .setAggregator(aggregator)
      .setMapSideCombine(true)
      .setKeyOrdering(BytesSerializableOrding)

    val c1 = shuffledPuts.flatMap {
        case (key, values) => {
          sortKeyValue(values.map(kv => new KeyValue(kv))).map(u => (new ImmutableBytesWritable(key.getBytes()), u))
        }
      }
      .saveAsNewAPIHadoopDataset(conf)

//    val c2 = puts.map(put => (new String(put.getRow), put.getRow)).groupByKey().count()
//    println("====================================")
//    println(c1)

  }

  @throws(classOf[IOException])
  def configureIncrementalLoad(conf: Configuration, table: Table, regionLocator: RegionLocator, bulkOut: String): Unit = {

    conf.set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, classOf[HFileOutputFormat2].getName)
    conf.set(MRJobConfig.OUTPUT_KEY_CLASS, classOf[BytesWritable].getName)
    conf.set(MRJobConfig.OUTPUT_VALUE_CLASS, classOf[KeyValue].getName)

    conf.set(org.apache.hadoop.mapreduce.lib.output.
      FileOutputFormat.OUTDIR, bulkOut)

    conf.setStrings("io.serializations", conf.get("io.serializations"), classOf[MutationSerialization].getName, classOf[ResultSerialization].getName, classOf[KeyValueSerialization].getName)
    configureCompression(table, conf)
    configureBloomType(table, conf)
    configureBlockSize(table, conf)
    configureDataBlockEncoding(table, conf)
    //    TableMapReduceUtil.initCredentials(job)
    logInfo("Incremental table " + table.getName + " output configured.")
  }

//  @throws(classOf[IOException])
//  @throws(classOf[InterruptedException])
//  def reduce(row: Array[Byte], puts: Iterable[Put]): Iterable[(Array[Byte], Cell)] = {
//    val iter: Iterator[Put] = puts.iterator
//    val ret: mutable.MutableList[(Array[Byte], Cell)] = mutable.MutableList()
//    import scala.collection.JavaConversions._
//    if (iter.hasNext) {
//      val map: util.TreeSet[KeyValue] = new util.TreeSet[KeyValue](KeyValue.COMPARATOR)
//      while (iter.hasNext) {
//        val p: Put = iter.next
//
//        for (cells <- p.getFamilyCellMap.values) {
//          for (cell <- cells) {
//            val kv: KeyValue = KeyValueUtil.ensureKeyValue(cell)
//            map.add(kv)
//          }
//        }
//      }
//
//      for (kv <- map) {
//        ret.+=((row, kv))
//      }
//    }
//    ret
//  }

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  def putToBytes(put: Put): Iterable[(Array[Byte], Array[Byte])] = {
    val ret: mutable.MutableList[(Array[Byte], Array[Byte])] = mutable.MutableList()
    import scala.collection.JavaConversions._

    for (cells <- put.getFamilyCellMap.values) {
      for (cell <- cells) {
        ret.+=((put.getRow, KeyValueUtil.copyToNewByteArray(KeyValueUtil.ensureKeyValue(cell))))
      }
    }
    ret
  }

//  @throws(classOf[IOException])
//  @throws(classOf[InterruptedException])
//  def reduceCellBytes(row: Array[Byte], cells: Iterable[Array[Byte]]): Iterable[(Array[Byte], Array[Byte])] = {
//    val iter: Iterator[Array[Byte]] = cells.iterator
//    val ret: mutable.MutableList[(Array[Byte], Array[Byte])] = mutable.MutableList()
//    import scala.collection.JavaConversions._
//    if (!iter.isEmpty) {
//      val map: util.TreeSet[Array[Byte]] = new util.TreeSet[Array[Byte]](Bytes.BYTES_COMPARATOR)
//      for (cell <- iter) {
//        map.add(cell)
//      }
//
//      for (kv <- map) {
//        ret.+=((row, kv))
//      }
//    }
//    ret
//  }

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  def sortKeyValue(kvs: Iterable[KeyValue]): Iterable[KeyValue] = {
    val ret: mutable.MutableList[KeyValue] = mutable.MutableList()
    import scala.collection.JavaConversions._
    val map: util.TreeSet[KeyValue] = new util.TreeSet[KeyValue](KeyValue.COMPARATOR)

    for (cell <- kvs) {
      map.add(cell)
    }

    for (kv <- map) {
      ret.+=(kv)
    }
    ret
  }

  def getRegionStartKeys(table: RegionLocator): List[Array[Byte]] = {
    val byteKeys: Array[Array[Byte]] = table.getStartKeys
    val ret: util.ArrayList[Array[Byte]] = new util.ArrayList[Array[Byte]](byteKeys.length)
    for (byteKey <- byteKeys) {
      ret.add(byteKey)
    }
    import scala.collection.JavaConverters._
    ret.asScala.toList
  }

  /**
    * Serialize column family to block size map to configuration.
    * Invoked while configuring the MR job for incremental load.
    *
    * @param table to read the properties from
    * @param conf to persist serialized values into
    * @throws IOException
   * on failure to read column family descriptors
    */
  @throws(classOf[IOException])
  def configureBlockSize(table: Table, conf: Configuration) {
    val blockSizeConfigValue: StringBuilder = new StringBuilder
    val tableDescriptor: HTableDescriptor = table.getTableDescriptor
    if (tableDescriptor == null) {
      return
    }
    val families: util.Collection[HColumnDescriptor] = tableDescriptor.getFamilies
    var i: Int = 0
    import scala.collection.JavaConversions._
    for (familyDescriptor <- families) {
      if (({
        i += 1
        i - 1
      }) > 0) {
        blockSizeConfigValue.append('&')
      }
      blockSizeConfigValue.append(URLEncoder.encode(familyDescriptor.getNameAsString, "UTF-8"))
      blockSizeConfigValue.append('=')
      blockSizeConfigValue.append(URLEncoder.encode(String.valueOf(familyDescriptor.getBlocksize), "UTF-8"))
    }
    conf.set(BLOCK_SIZE_FAMILIES_CONF_KEY, blockSizeConfigValue.toString)
  }

  /**
    * Serialize column family to bloom type map to configuration.
    * Invoked while configuring the MR job for incremental load.
    *
    * @param table to read the properties from
    * @param conf to persist serialized values into
    * @throws IOException
   * on failure to read column family descriptors
    */
  @throws(classOf[IOException])
  def configureBloomType(table: Table, conf: Configuration) {
    val tableDescriptor: HTableDescriptor = table.getTableDescriptor
    if (tableDescriptor == null) {
      return
    }
    val bloomTypeConfigValue: StringBuilder = new StringBuilder
    val families: util.Collection[HColumnDescriptor] = tableDescriptor.getFamilies
    var i: Int = 0
    import scala.collection.JavaConversions._
    for (familyDescriptor <- families) {
      if (({
        i += 1
        i - 1
      }) > 0) {
        bloomTypeConfigValue.append('&')
      }
      bloomTypeConfigValue.append(URLEncoder.encode(familyDescriptor.getNameAsString, "UTF-8"))
      bloomTypeConfigValue.append('=')
      var bloomType: String = familyDescriptor.getBloomFilterType.toString
      if (bloomType == null) {
        bloomType = HColumnDescriptor.DEFAULT_BLOOMFILTER
      }
      bloomTypeConfigValue.append(URLEncoder.encode(bloomType, "UTF-8"))
    }
    conf.set(BLOOM_TYPE_FAMILIES_CONF_KEY, bloomTypeConfigValue.toString)
  }

  /**
    * Serialize column family to compression algorithm map to configuration.
    * Invoked while configuring the MR job for incremental load.
    *
    * @param table to read the properties from
    * @param conf to persist serialized values into
    * @throws IOException
   * on failure to read column family descriptors
    */
  @throws(classOf[IOException])
  def configureCompression(table: Table, conf: Configuration) {
    val compressionConfigValue: StringBuilder = new StringBuilder
    val tableDescriptor: HTableDescriptor = table.getTableDescriptor
    if (tableDescriptor == null) {
      return
    }
    val families: util.Collection[HColumnDescriptor] = tableDescriptor.getFamilies
    var i: Int = 0
    import scala.collection.JavaConversions._
    for (familyDescriptor <- families) {
      if (({
        i += 1
        i - 1
      }) > 0) {
        compressionConfigValue.append('&')
      }
      compressionConfigValue.append(URLEncoder.encode(familyDescriptor.getNameAsString, "UTF-8"))
      compressionConfigValue.append('=')
      compressionConfigValue.append(URLEncoder.encode(familyDescriptor.getCompression.getName, "UTF-8"))
    }
    conf.set(COMPRESSION_FAMILIES_CONF_KEY, compressionConfigValue.toString)
  }

  /**
    * Serialize column family to data block encoding map to configuration.
    * Invoked while configuring the MR job for incremental load.
    *
    * @param table to read the properties from
    * @param conf to persist serialized values into
    * @throws IOException
   * on failure to read column family descriptors
    */
  @throws(classOf[IOException])
  def configureDataBlockEncoding(table: Table, conf: Configuration) {
    val tableDescriptor: HTableDescriptor = table.getTableDescriptor
    if (tableDescriptor == null) {
      return
    }
    val dataBlockEncodingConfigValue: StringBuilder = new StringBuilder
    val families: util.Collection[HColumnDescriptor] = tableDescriptor.getFamilies
    var i: Int = 0
    import scala.collection.JavaConversions._
    for (familyDescriptor <- families) {
      if (({
        i += 1
        i - 1
      }) > 0) {
        dataBlockEncodingConfigValue.append('&')
      }
      dataBlockEncodingConfigValue.append(URLEncoder.encode(familyDescriptor.getNameAsString, "UTF-8"))
      dataBlockEncodingConfigValue.append('=')
      var encoding: DataBlockEncoding = familyDescriptor.getDataBlockEncoding
      if (encoding == null) {
        encoding = DataBlockEncoding.NONE
      }
      dataBlockEncodingConfigValue.append(URLEncoder.encode(encoding.toString, "UTF-8"))
    }
    conf.set(DATABLOCK_ENCODING_FAMILIES_CONF_KEY, dataBlockEncodingConfigValue.toString)
  }
}
