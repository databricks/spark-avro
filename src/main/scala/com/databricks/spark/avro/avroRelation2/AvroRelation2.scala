package com.databricks.spark.avro.avroRelation2

import java.io.FileNotFoundException
import java.util.zip.Deflater

import com.databricks.spark.avro.{AvroSaver, SchemaConverters}
import com.google.common.base.Objects
import org.apache.avro.Schema.Field
import org.apache.avro.file.{DataFileReader, FileReader}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.mapred.{AvroOutputFormat, FsInput}
import org.apache.avro.mapreduce.AvroJob
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.compress.{DeflateCodec, SnappyCodec}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import parquet.hadoop.util.ContextUtil

import scala.collection.Iterator
import scala.collection.JavaConversions._


object AvroRelation2 {
  final val RECORD_NAME = "recordName"
  final val RECORD_NAMESPACE = "recordNamespace"
  final val AVRO_COMPRESSION_CODEC = "spark.sql.avro.compression.codec"
  final val AVRO_DEFLATE_LEVEL = "spark.sql.avro.deflate.level"
}

class AvroRelation2(override val paths: Array[String],
                   private val maybeDataSchema: Option[StructType],
                   override val userDefinedPartitionColumns: Option[StructType],
                   private val parameters: Map[String, String])
                  (@transient val sqlContext: SQLContext) extends HadoopFsRelation with Logging {

  private final val IgnoreFilesWithoutExtensionProperty = "avro.mapred.ignore.inputs.without.extension"
  private val recordName = parameters.getOrElse(AvroRelation2.RECORD_NAME,
    AvroSaver.defaultParameters.get(AvroRelation2.RECORD_NAME).get)
  private val recordNamespace = parameters.getOrElse(AvroRelation2.RECORD_NAMESPACE,
    AvroSaver.defaultParameters.get(AvroRelation2.RECORD_NAMESPACE).get)

  /** needs to be lazy so it is not evaluated when saving since no schema exists at that location */
  private lazy val avroSchema: Schema = paths match {
    case Array(head, _*) => newReader(head)(_.getSchema)
    case Array() => sys.error("no file paths given")
  }

  /**
   * Specifies schema of actual data files.  For partitioned relations, if one or more partitioned
   * columns are contained in the data files, they should also appear in `dataSchema`.
   *
   * @since 1.4.0
   */
  override lazy val dataSchema: StructType = maybeDataSchema match {
    case Some(structType) => structType
    case None => SchemaConverters.toSqlType(avroSchema).dataType match {
      case s: StructType => s
      case other => sys.error(s"Avro files must contain Records to be read, type $other not supported")
    }
  }

  /**
   * Prepares a write job and returns an [[OutputWriterFactory]].  Client side job preparation can
   * be put here.  For example, user defined output committer can be configured here
   * by setting the output committer class in the conf of spark.sql.sources.outputCommitterClass.
   *
   * Note that the only side effect expected here is mutating `job` via its setters.  Especially,
   * Spark SQL caches [[BaseRelation]] instances for performance, mutating relation internal states
   * may cause unexpected behaviors.
   *
   * @since 1.4.0
   */
  override def prepareJobForWrite(job: Job): OutputWriterFactory = {
    val builder = SchemaBuilder.record(recordName).namespace(recordNamespace)
    val outputAvroSchema = SchemaConverters.convertStructToAvro(dataSchema, builder, recordNamespace)
    AvroJob.setOutputKeySchema(job, outputAvroSchema)

    sqlContext.getConf(AvroRelation2.AVRO_COMPRESSION_CODEC, "snappy") match {
      case "uncompressed" =>
        logInfo("writing Avro out uncompressed")
        FileOutputFormat.setCompressOutput(job, false)
      case "snappy" =>
        logInfo("using snappy for Avro output")
        FileOutputFormat.setOutputCompressorClass(job, classOf[SnappyCodec])
      case "deflate" =>
        val deflateLevel = sqlContext.getConf(AvroRelation2.AVRO_DEFLATE_LEVEL,
                                              Deflater.DEFAULT_COMPRESSION.toString).toInt
        logInfo(s"using deflate: $deflateLevel for Avro output")
        FileOutputFormat.setOutputCompressorClass(job, classOf[DeflateCodec])
        ContextUtil.getConfiguration(job).setInt(AvroOutputFormat.DEFLATE_LEVEL_KEY, deflateLevel)
      case unknown: String => sys.error(s"Unknown output compression: $unknown")
    }
    new AvroOutputWriterFactory(schema, recordName, recordNamespace)
  }


  /**
   * This filters out unneeded columns before converting into the internal row representation.
   * The first record is used to get the sub-schema that contains only the requested fields, this is then used
   * to generate the field converters and the rows that only contain `requiredColumns`
   */
  override def buildScan(requiredColumns: Array[String], inputs: Array[FileStatus]): RDD[Row] = {
    if (inputs.isEmpty) {
      sqlContext.sparkContext.emptyRDD[Row]
    } else {
      inputs.map(path =>
        sqlContext.sparkContext.hadoopFile(
          path.getPath.toString,
          classOf[org.apache.avro.mapred.AvroInputFormat[GenericRecord]],
          classOf[org.apache.avro.mapred.AvroWrapper[GenericRecord]],
          classOf[org.apache.hadoop.io.NullWritable]).keys.map(_.datum())
          .mapPartitions { records =>
            if (records.isEmpty) {
              Iterator.empty
            } else {
              val first = records.next()
              val superSchema = first.getSchema // the schema of the actual record
              val fields = superSchema.getFields // the fields that are actually required with their SQL converters
                  .filter(field => requiredColumns.contains(field.name))
                  .map { field =>
                    val newField = new Field(field.name, field.schema, field.doc, field.defaultValue, field.order)
                    (SchemaConverters.createConverterToSQL(newField.schema()), newField)
                  }
              Iterator(Row.fromSeq(fields.map(f => f._1(first.get(f._2.name))))) ++
                records.map(record => Row.fromSeq(fields.map(f => f._1(record.get(f._2.name)))))
            }
        }).reduce(_ ++ _)
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: AvroRelation2 => paths.toSet == that.paths.toSet &&
                                dataSchema == that.dataSchema &&
                                schema == that.schema &&
                                partitionColumns == that.partitionColumns
    case _ => false
  }

  override def hashCode(): Int = Objects.hashCode(paths.toSet, dataSchema, schema, partitionColumns)

  /**
   * Opens up the location to for reading. Takes in a function to run on the schema and returns the
   * result of this function. This takes in a function so that the caller does not have to worry
   * about cleaning up and closing the reader.
   * @param location the location in the filesystem to read from
   * @param fun the function that is called on when the reader has been initialized
   * @tparam T the return type of the function given
   */
  private def newReader[T](location: String)(fun: FileReader[GenericRecord] => T): T = {
    val path = new Path(location)
    val hadoopConfiguration = sqlContext.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(path.toUri, hadoopConfiguration)

    val statuses = fs.globStatus(path) match {
      case null => throw new FileNotFoundException(s"The path you've provided ($location) is invalid.")
      case globStatus => globStatus.toStream.map(_.getPath).flatMap(getAllFiles(fs)(_))
    }

    val singleFile =
      (if (hadoopConfiguration.getBoolean(IgnoreFilesWithoutExtensionProperty, true)) {
        statuses.find(_.getName.endsWith("avro"))
      } else {
        statuses.headOption
      }).getOrElse(sys.error(s"Could not find .avro file with schema at $path"))

    val reader = DataFileReader.openReader(new FsInput(singleFile, hadoopConfiguration),
      new GenericDatumReader[GenericRecord]())
    val result = fun(reader)
    reader.close()
    result
  }

  private def getAllFiles(fs: FileSystem)(path: Path): Stream[Path] = {
    if (fs.isDirectory(path)) {
      fs.listStatus(path).toStream.map(_.getPath).flatMap(getAllFiles(fs)(_))
    } else {
      Stream(path)
    }
  }
}




