/*
 * Copyright 2014 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.databricks.spark.avro

import java.io.ByteArrayOutputStream
import java.util.function.Supplier

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, GenericInternalRow, GenericRow, UnaryExpression}
import org.apache.spark.sql.types.{BinaryType, DataType, StructType}
import org.slf4j.LoggerFactory

package object functions {
  /**
    * Parses a column containing a AVRO record into a `StructType` with the
    * schema derived from avro schema. Returns `null`, in the case of a parse error.
    *
    * @param e a byte array column containing AVRO data.
    * @param avroSchema the avro schema to use when parsing the byte array
    * @param offsetBytes If the input byte array has a extra information block
    *                    before the avro record this option allows to specify the length of this block.
    *                    This function will simply skip the number of bytes from the input array.
    *
    * @since 4.1.0
    */
  def from_avro(e: Column, avroSchema: String, offsetBytes: Int = 0): Column =
    new Column(AvroToStruct(avroSchema, e.expr, offsetBytes))

  /**
    * Converts an byte array to a [[StructType]] derived from the specified AVRO schema.
    */
  private[functions] case class AvroToStruct(schema: String, child: Expression,
                                             offsetBytes: Int = 0)
    extends UnaryExpression with CodegenFallback with ExpectsInputTypes {
    override def nullable: Boolean = true

    @transient
    private lazy val avroSchema = new Schema.Parser().parse(schema)
    @transient
    private lazy val sqlSchema: SchemaConverters.SchemaType = SchemaConverters.toSqlType(avroSchema)
    @transient
    private lazy val recordConverter = SchemaConverters.createConverterToSQL(avroSchema, dataType)
    @transient
    private lazy val encoderForDataColumns = RowEncoder(sqlSchema.dataType.asInstanceOf[StructType])

    override def dataType: DataType = sqlSchema.dataType

    override def nullSafeEval(byteArr: Any): Any = {
      try parse(byteArr.asInstanceOf[Array[Byte]]) catch {
        case x: Exception =>
          LoggerFactory.getLogger(getClass).warn("Unable to parse avro record")
          null
      }
    }

    private def parse(payload: Array[Byte]): Any = {
      val genericRecord = deserialize(avroSchema, payload, offsetBytes)
      val row = recordConverter.apply(genericRecord).asInstanceOf[GenericRow]
      val internalRow = encoderForDataColumns.toRow(row)
      internalRow.copy()
    }

    override def inputTypes: Seq[org.apache.spark.sql.types.DataType] = BinaryType :: Nil
  }

  private[functions] def deserialize(avroSchema: Schema, payload: Array[Byte],
                                     offsetBytes: Int): GenericRecord = {
    val recordLength = payload.length - offsetBytes
    val decoder = DecoderFactory.get.binaryDecoder(payload, offsetBytes, recordLength, null)
    val datumReader = new GenericDatumReader[GenericRecord](avroSchema)

    datumReader.read(null, decoder)
  }
}
