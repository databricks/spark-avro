package com.databricks.spark.avro.avroRelation2

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{HadoopFsRelation, HadoopFsRelationProvider}
import org.apache.spark.sql.types.StructType

private[avro] class DefaultSource extends HadoopFsRelationProvider {

  def createRelation(sqlContext: SQLContext,
                     paths: Array[String],
                     dataSchema: Option[StructType],
                     partitionColumns: Option[StructType],
                     parameters: Map[String, String]): HadoopFsRelation = {
    new AvroRelation2(paths, dataSchema, partitionColumns, parameters)(sqlContext)
  }
}

