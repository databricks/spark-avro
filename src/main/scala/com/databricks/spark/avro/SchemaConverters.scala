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

import java.nio.ByteBuffer
import java.util.HashMap
import scala.collection.JavaConversions._
import org.apache.avro.generic.GenericData.Fixed
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.SchemaBuilder._
import org.apache.avro.Schema.Type._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import java.util.ArrayList
import scala.collection.mutable.ListBuffer


/**
 * This object contains method that are used to convert sparkSQL schemas to avro schemas and vice
 * versa.
 */
private object SchemaConverters {

  val METADATA_KEY_DOC = "doc";
  val METADATA_KEY_ALIASES = "aliases";
  val METADATA_KEY_PARENT = "_parent";

  case class SchemaType(dataType: DataType, nullable: Boolean)

  /**
   * This function takes an avro schema and returns a sql schema.
   */
  private[avro] def toSqlType(avroSchema: Schema, schemaWithAlias: Boolean = false): SchemaType = {
    var aliasFields = ListBuffer[StructField]()
    avroSchema.getType match {
      case INT => SchemaType(IntegerType, nullable = false)
      case STRING => SchemaType(StringType, nullable = false)
      case BOOLEAN => SchemaType(BooleanType, nullable = false)
      case BYTES => SchemaType(BinaryType, nullable = false)
      case DOUBLE => SchemaType(DoubleType, nullable = false)
      case FLOAT => SchemaType(FloatType, nullable = false)
      case LONG => SchemaType(LongType, nullable = false)
      case FIXED => SchemaType(BinaryType, nullable = false)
      case ENUM => SchemaType(StringType, nullable = false)

      case RECORD =>
        val fields = avroSchema.getFields.map { f =>
          val schemaType = toSqlType(f.schema())
          var meta = new MetadataBuilder()
          if (f.doc != null) meta.putString(METADATA_KEY_DOC, f.doc)
          if (f.aliases() != null && f.aliases().size() > 0) {
            val aliasArray = new Array[String](f.aliases().size())
            meta.putString(METADATA_KEY_PARENT, f.name)
            f.aliases copyToArray(aliasArray)
            meta.putStringArray(METADATA_KEY_ALIASES, aliasArray);
            if (schemaWithAlias) {
              for (aliasFieldName <- aliasArray) {
                aliasFields += StructField(aliasFieldName, schemaType.dataType,
                  schemaType.nullable, meta.build())
              }
            }
          }
          StructField(f.name, schemaType.dataType, schemaType.nullable, meta.build())
        }
        fields.addAll(aliasFields)
        SchemaType(StructType(fields), nullable = false)

      case ARRAY =>
        val schemaType = toSqlType(avroSchema.getElementType)
        SchemaType(
          ArrayType(schemaType.dataType, containsNull = schemaType.nullable),
          nullable = false)

      case MAP =>
        val schemaType = toSqlType(avroSchema.getValueType)
        SchemaType(
          MapType(StringType, schemaType.dataType, valueContainsNull = schemaType.nullable),
          nullable = false)

      case UNION =>
        if (avroSchema.getTypes.exists(_.getType == NULL)) {
          // In case of a union with null, eliminate it and make a recursive call
          val remainingUnionTypes = avroSchema.getTypes.filterNot(_.getType == NULL)
          if (remainingUnionTypes.size == 1) {
            toSqlType(remainingUnionTypes.get(0)).copy(nullable = true)
          } else {
            toSqlType(Schema.createUnion(remainingUnionTypes)).copy(nullable = true)
          }
        } else avroSchema.getTypes.map(_.getType) match {
          case Seq(t1, t2) if Set(t1, t2) == Set(INT, LONG) =>
            SchemaType(LongType, nullable = false)
          case Seq(t1, t2) if Set(t1, t2) == Set(FLOAT, DOUBLE) =>
            SchemaType(DoubleType, nullable = false)
          case other => throw new SchemaConversionException(
            s"This mix of union types is not supported (see README): $other")
        }

      case other => throw new SchemaConversionException(s"Unsupported type $other")
    }
  }

  def dataFrameWithAliasColumn(df : DataFrame) : DataFrame = {
    var newDf = df
    for (field <- df.schema.fields) {
      if (field.metadata.contains(METADATA_KEY_ALIASES)) {
        val aliasArray = field.metadata.getStringArray(METADATA_KEY_ALIASES)
        for (alias <- aliasArray) {
          newDf = newDf.withColumn(alias, df.col(field.name))
        }
      }
    }
    newDf
  }

  /**
   * This function converts sparkSQL StructType into avro schema. This method uses two other
   * converter methods in order to do the conversion.
   */
  private[avro] def convertStructToAvro[T](
      structType: StructType,
      schemaBuilder: RecordBuilder[T],
      recordNamespace: String): T = {
    val fieldsAssembler: FieldAssembler[T] = schemaBuilder.fields()

    val nonAliasStructFields = structType.fields.filterNot(field =>
      field.metadata.contains(METADATA_KEY_ALIASES)
        && field.metadata.contains(METADATA_KEY_PARENT)
            && !field.metadata.getString(METADATA_KEY_PARENT).equals(field.name))

    nonAliasStructFields.foreach { field =>
      var newFieldBuilder = fieldsAssembler.name(field.name)
      if (field.metadata contains (METADATA_KEY_DOC)) {
        newFieldBuilder = newFieldBuilder.doc(field.metadata.getString(METADATA_KEY_DOC))
      }
      if (field.metadata.contains(METADATA_KEY_ALIASES)) {
        newFieldBuilder = newFieldBuilder
          .aliases(field.metadata.getStringArray(METADATA_KEY_ALIASES): _*)
      }
      val newField = newFieldBuilder.`type`()
      if (field.nullable) {
        convertFieldTypeToAvro(field.dataType, newField.nullable(), field.name, recordNamespace)
          .noDefault
      } else {
        convertFieldTypeToAvro(field.dataType, newField, field.name, recordNamespace)
          .noDefault
      }
    }
    fieldsAssembler.endRecord()
  }

  /**
   * Returns a function that is used to convert avro types to their
   * corresponding sparkSQL representations.
   */
  private[avro] def createConverterToSQL(schema: Schema): Any => Any = {
    schema.getType match {
      // Avro strings are in Utf8, so we have to call toString on them
      case STRING | ENUM => (item: Any) => if (item == null) null else item.toString
      case INT | BOOLEAN | DOUBLE | FLOAT | LONG => identity
      // Byte arrays are reused by avro, so we have to make a copy of them.
      case FIXED => (item: Any) => if (item == null) {
        null
      } else {
        item.asInstanceOf[Fixed].bytes().clone()
      }
      case BYTES => (item: Any) => if (item == null) {
        null
      } else {
        val bytes = item.asInstanceOf[ByteBuffer]
        val javaBytes = new Array[Byte](bytes.remaining)
        bytes.get(javaBytes)
        javaBytes
      }
      case RECORD =>
        val fieldConverters = schema.getFields.map(f => createConverterToSQL(f.schema))
        (item: Any) => if (item == null) {
          null
        } else {
          val record = item.asInstanceOf[GenericRecord]
          val converted = new Array[Any](fieldConverters.size)
          var idx = 0
          while (idx < fieldConverters.size) {
            converted(idx) = fieldConverters.apply(idx)(record.get(idx))
            idx += 1
          }
          Row.fromSeq(converted.toSeq)
        }
      case ARRAY =>
        val elementConverter = createConverterToSQL(schema.getElementType)
        (item: Any) => if (item == null) {
          null
        } else {
          item.asInstanceOf[GenericData.Array[Any]].map(elementConverter)
        }
      case MAP =>
        val valueConverter = createConverterToSQL(schema.getValueType)
        (item: Any) => if (item == null) {
          null
        } else {
          item.asInstanceOf[HashMap[Any, Any]].map(x => (x._1.toString, valueConverter(x._2))).toMap
        }
      case UNION =>
        if (schema.getTypes.exists(_.getType == NULL)) {
          val remainingUnionTypes = schema.getTypes.filterNot(_.getType == NULL)
          if (remainingUnionTypes.size == 1) {
            createConverterToSQL(remainingUnionTypes.get(0))
          } else {
            createConverterToSQL(Schema.createUnion(remainingUnionTypes))
          }
        } else schema.getTypes.map(_.getType) match {
          case Seq(t1, t2) if Set(t1, t2) == Set(INT, LONG) =>
            (item: Any) => {
              item match {
                case l: Long => l
                case i: Int => i.toLong
                case null => null
              }
            }
          case Seq(t1, t2) if Set(t1, t2) == Set(FLOAT, DOUBLE) =>
            (item: Any) => {
              item match {
                case d: Double => d
                case f: Float => f.toDouble
                case null => null
              }
            }
          case other => throw new SchemaConversionException(
            s"This mix of union types is not supported (see README): $other")
        }
      case other => throw new SchemaConversionException(s"invalid avro type: $other")
    }
  }

  /**
   * This function is used to convert some sparkSQL type to avro type. Note that this function won't
   * be used to construct fields of avro record (convertFieldTypeToAvro is used for that).
   */
  private def convertTypeToAvro[T](
      dataType: DataType,
      schemaBuilder: BaseTypeBuilder[T],
      structName: String,
      recordNamespace: String): T = {
    dataType match {
      case ByteType => schemaBuilder.intType()
      case ShortType => schemaBuilder.intType()
      case IntegerType => schemaBuilder.intType()
      case LongType => schemaBuilder.longType()
      case FloatType => schemaBuilder.floatType()
      case DoubleType => schemaBuilder.doubleType()
      case _: DecimalType => schemaBuilder.stringType()
      case StringType => schemaBuilder.stringType()
      case BinaryType => schemaBuilder.bytesType()
      case BooleanType => schemaBuilder.booleanType()
      case TimestampType => schemaBuilder.longType()

      case ArrayType(elementType, _) =>
        val builder = getSchemaBuilder(dataType.asInstanceOf[ArrayType].containsNull)
        val elementSchema = convertTypeToAvro(elementType, builder, structName, recordNamespace)
        schemaBuilder.array().items(elementSchema)

      case MapType(StringType, valueType, _) =>
        val builder = getSchemaBuilder(dataType.asInstanceOf[MapType].valueContainsNull)
        val valueSchema = convertTypeToAvro(valueType, builder, structName, recordNamespace)
        schemaBuilder.map().values(valueSchema)

      case structType: StructType =>
        convertStructToAvro(
          structType,
          schemaBuilder.record(structName).namespace(recordNamespace),
          recordNamespace)

      case other => throw new IllegalArgumentException(s"Unexpected type $dataType.")
    }
  }

  /**
   * This function is used to construct fields of the avro record, where schema of the field is
   * specified by avro representation of dataType. Since builders for record fields are different
   * from those for everything else, we have to use a separate method.
   */
  private def convertFieldTypeToAvro[T](
      dataType: DataType,
      newFieldBuilder: BaseFieldTypeBuilder[T],
      structName: String,
      recordNamespace: String): FieldDefault[T, _] = {
    dataType match {
      case ByteType => newFieldBuilder.intType()
      case ShortType => newFieldBuilder.intType()
      case IntegerType => newFieldBuilder.intType()
      case LongType => newFieldBuilder.longType()
      case FloatType => newFieldBuilder.floatType()
      case DoubleType => newFieldBuilder.doubleType()
      case _: DecimalType => newFieldBuilder.stringType()
      case StringType => newFieldBuilder.stringType()
      case BinaryType => newFieldBuilder.bytesType()
      case BooleanType => newFieldBuilder.booleanType()
      case TimestampType => newFieldBuilder.longType()

      case ArrayType(elementType, _) =>
        val builder = getSchemaBuilder(dataType.asInstanceOf[ArrayType].containsNull)
        val elementSchema = convertTypeToAvro(elementType, builder, structName, recordNamespace)
        newFieldBuilder.array().items(elementSchema)

      case MapType(StringType, valueType, _) =>
        val builder = getSchemaBuilder(dataType.asInstanceOf[MapType].valueContainsNull)
        val valueSchema = convertTypeToAvro(valueType, builder, structName, recordNamespace)
        newFieldBuilder.map().values(valueSchema)

      case structType: StructType =>
        convertStructToAvro(
          structType,
          newFieldBuilder.record(structName).namespace(recordNamespace),
          recordNamespace)

      case other => throw new IllegalArgumentException(s"Unexpected type $dataType.")
    }
  }

  private def getSchemaBuilder(isNullable: Boolean): BaseTypeBuilder[Schema] = {
    if (isNullable) {
      SchemaBuilder.builder().nullable()
    } else {
      SchemaBuilder.builder()
    }
  }
}
