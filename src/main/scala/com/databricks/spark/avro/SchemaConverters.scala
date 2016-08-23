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


/**
 * This object contains method that are used to convert sparkSQL schemas to avro schemas and vice
 * versa.
 */
object SchemaConverters {

  private val dtCache = collection.mutable.Map[String, (Any => Any)]()
  
  case class SchemaType(dataType: DataType, nullable: Boolean)

  /**
   * This function takes an avro schema and returns a sql schema.
   */
  def toSqlType(avroSchema: Schema): SchemaType = {
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
          StructField(f.name, schemaType.dataType, schemaType.nullable)
        }

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
          case Seq(t1) =>
            toSqlType(avroSchema.getTypes.get(0))
          case Seq(t1, t2) if Set(t1, t2) == Set(INT, LONG) =>
            SchemaType(LongType, nullable = false)
          case Seq(t1, t2) if Set(t1, t2) == Set(FLOAT, DOUBLE) =>
            SchemaType(DoubleType, nullable = false)
          case _ =>
            // Convert complex unions to struct types where field names are member0, member1, etc.
            // This is consistent with the behavior when reading Parquet files.
            val fields = avroSchema.getTypes.zipWithIndex map {
              case (s, i) =>
                val schemaType = toSqlType(s)
                // All fields are nullable because only one of them is set at a time
                StructField(s"member$i", schemaType.dataType, nullable = true)
            }

            SchemaType(StructType(fields), nullable = false)
          //case other => throw new UnsupportedOperationException(
          //  s"This mix of union types is not supported (see README): $other")
        }

      case other => throw new UnsupportedOperationException(s"Unsupported type $other")
    }
  }

  /**
   * This function converts sparkSQL StructType into avro schema. This method uses two other
   * converter methods in order to do the conversion.
   */
  def convertStructToAvro[T](
      structType: StructType,
      schemaBuilder: RecordBuilder[T],
      recordNamespace: String): T = {
    val fieldsAssembler: FieldAssembler[T] = schemaBuilder.fields()
    structType.fields.foreach { field =>
      val newField = fieldsAssembler.name(field.name).`type`()
      if (field.nullable) {
        convertFieldTypeToAvro(field.dataType, newField.nullable(), field.name, recordNamespace).noDefault
      } else {
        convertFieldTypeToAvro(field.dataType, newField, field.name, recordNamespace).noDefault
      }
    }
    fieldsAssembler.endRecord()
  }

  def createConverterToAvro(dataType: DataType, avroSchema: AvroSchema) : (Any) => Any = 
    dtCache.getOrElseUpdate(dataType.typeName, createConverterToAvro(dataType, avroSchema.schema))
  
  def convert(dataType: DataType, schema: AvroSchema, row: Row): GenericRecord = {
    createConverterToAvro(dataType, schema)(row).asInstanceOf[GenericRecord]
  }
  
  private[avro] def createConverterToAvro(dataType: DataType, schema: Schema): (Any) => Any = {
    dataType match {
      case BinaryType => (item: Any) => item match {
        case null => null
        case bytes: Array[Byte] => ByteBuffer.wrap(bytes)
      }
      case ByteType | ShortType | IntegerType | LongType |
           FloatType | DoubleType | BooleanType => identity
      case StringType => {
        // TEMP SUPPORT FOR (UNIONS OF) ENUMS OF STRINGS
        val elementSchema = schema.getType match {
          case UNION => {
            val remainingTypes = schema.getTypes.filterNot(_.getType == NULL)
            if (remainingTypes.length != 1) throw new UnsupportedOperationException("Unsupported: Avro Union can only represent a SQL ArrayType if it's used for optional elements (null)")
            else {
              remainingTypes.get(0)
            }
          }
          case _ => schema
        }
        elementSchema.getType match {
          case ENUM => {
            (item: Any) => {
              println("IT IS AN ENUM: " + item)
              if (item == null) null
              else new GenericData.EnumSymbol(elementSchema, item.asInstanceOf[String])
            }
          }
          case _ => identity
        }
      }
      case _: DecimalType => (item: Any) => if (item == null) null else item.toString
      case TimestampType => (item: Any) =>
        if (item == null) null else item.asInstanceOf[java.sql.Timestamp].getTime
      case ArrayType(elementType, containsNull) =>
        val elementSchema = schema.getType match {
          case UNION => {
            val remainingTypes = schema.getTypes.filterNot(_.getType == NULL)
            if (remainingTypes.length != 1) throw new UnsupportedOperationException("Unsupported: Avro Union can only represent a SQL ArrayType if it's used for optional elements (null)")
            else remainingTypes.get(0).getElementType // DOES THIS WORK? not remainingTypes.get(0).getType instead?
          }
          case ARRAY => {
            schema.getElementType
          }
          case _ => {
            throw new UnsupportedOperationException("Unsupported: can't match an Avro " + schema.getType + " to a SQL ArrayType")
          }
        }
        val elementConverter = createConverterToAvro(elementType, elementSchema)
        (item: Any) => {
          if (item == null )  null
          else {
            val sourceArray = item.asInstanceOf[Seq[Any]]
            val sourceArraySize = sourceArray.size
            val targetList = new java.util.ArrayList[Any]
            var idx = 0
            while (idx < sourceArraySize) {
              targetList.add(idx, elementConverter(sourceArray(idx)))
              idx += 1
            }
            targetList
          }
        }
      // Avro maps only support string keys
      case MapType(StringType, valueType, _) =>        
        val valueConverter = createConverterToAvro(valueType, schema.getValueType)
        (item: Any) => {
          if (item == null) null
          else {
            val javaMap = new HashMap[String, Any]()
            item.asInstanceOf[Map[String, Any]].foreach { case (key, value) =>
              javaMap.put(key, valueConverter(value))
            }
            javaMap
          }
        }
      case structType: StructType => {
        //println("** Struct matching schema " + schema.getType)
        schema.getType match {
          case UNION => {
            if (schema.getTypes.exists(_.getType == NULL)) {
              val remainingUnionTypes = schema.getTypes.filterNot(_.getType == NULL)
              if (remainingUnionTypes.size == 1) {
                //println("PROCESSING UNION createConverterToAvro with " + structType.toString() + ", schema of field is " + remainingUnionTypes.get(0))
                createConverterToAvro(structType, remainingUnionTypes.get(0))
              }
              else {
                // TODO
                throw new UnsupportedOperationException(s"1111 This mix of union types is not supported (see README)")
              }
            }
            else {
              // TODO
              throw new UnsupportedOperationException(s"2222 This mix of union types is not supported (see README)")
            }
          }
          case _ => {
            val fieldConverters = structType.fields.map(f => {
              val s = schema.getField(f.name).schema()
              //println("PROCESSING _ createConverterToAvro with " + f.dataType.toString() + ", schema of field " + f.name + " is " + s)
              createConverterToAvro(f.dataType, s)
            })
            (item: Any) => {
              if (item == null) null
              else {
                //println("FUNCTION for struct " + item)
                val record = new GenericData.Record(schema)
                val convertersIterator = fieldConverters.iterator
                val fieldNamesIterator = dataType.asInstanceOf[StructType].fieldNames.iterator
                val rowIterator = item.asInstanceOf[Row].toSeq.iterator
    
                while (convertersIterator.hasNext) {
                  val converter = convertersIterator.next()
                  record.put(fieldNamesIterator.next(), converter(rowIterator.next()))
                }
                record
              }
            }
          }
        }
        
      }

    }
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
          val converted = item.asInstanceOf[java.util.Collection[Any]]
          val genericWrapper = new GenericData.Array(schema, converted)
          genericWrapper.map(elementConverter)
          /*
          item match { 
            case c : java.util.Collection[Any] @unchecked => c.map(elementConverter)
            case _ => item.asInstanceOf[GenericData.Array[Any]].map(elementConverter)
          }
          * 
          */
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
          case Seq(t1) =>
            createConverterToSQL(schema.getTypes.get(0))
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
          case _ =>
            val fieldConverters = schema.getTypes map createConverterToSQL
            (item: Any) => if (item == null) {
              null
            } else {
              val i = GenericData.get().resolveUnion(schema, item)
              val converted = new Array[Any](fieldConverters.size)
              converted(i) = fieldConverters(i)(item)
              Row.fromSeq(converted.toSeq)
            }
        }
      case other => throw new UnsupportedOperationException(s"invalid avro type: $other")
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
          schemaBuilder.record(recordNamespace + "." + structName),
          recordNamespace + "." + structName)

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
          newFieldBuilder.record(recordNamespace + "." + structName),
          recordNamespace + "." + structName)

      case other => throw new UnsupportedOperationException(s"Unexpected type $dataType.")
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
