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

/**
 * A Spark-SQL Encoder for Avro objects
 */
import java.util.{Map => JMap}
import scala.collection.JavaConverters._
import com.databricks.spark.avro.SchemaConverters.{IncompatibleSchemaException, SchemaType, resolveUnionType, toSqlType}
import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.{GenericData, IndexedRecord}
import org.apache.avro.reflect.ReflectData
import org.apache.avro.specific.SpecificRecord
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{GetColumnByOrdinal, UnresolvedAttribute, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
 * A Spark-SQL Encoder for Avro objects
 */
object AvroEncoder {
  /**
   * Provides an Encoder for Avro objects of the given class
   * @param avroClass the class of the Avro object for which to generate the Encoder
   * @tparam T the type of the Avro class, must implement SpecificRecord
   * @return an Encoder for the given Avro class
   */
  def of[T <: SpecificRecord](avroClass: Class[T]): Encoder[T] = {
    AvroExpressionEncoder.of(avroClass)
  }

  /**
   * Provides an Encoder for Avro objects implementing the given schema
   * @param avroSchema the Schema of the Avro object for which to generate the Encoder
   * @tparam T the type of the Avro class that implements the Schema, must implement IndexedRecord
   * @return an Encoder for the given Avro Schema
   */
  def of[T <: IndexedRecord](avroSchema: Schema): Encoder[T] = {
    AvroExpressionEncoder.of(avroSchema)
  }
}

object AvroExpressionEncoder {
  def of[T <: SpecificRecord](avroClass: Class[T]): ExpressionEncoder[T] = {
    val schema = avroClass.getMethod("getClassSchema").invoke(null).asInstanceOf[Schema]
    assert(toSqlType(schema).dataType.isInstanceOf[StructType])

    val serializer = AvroTypeInference.serializerFor(avroClass, schema)
    val deserializer = AvroTypeInference.deserializerFor(schema)

    new ExpressionEncoder[T](
      toSqlType(schema).dataType.asInstanceOf[StructType],
      flat = false,
      serializer.flatten,
      deserializer = deserializer,
      ClassTag[T](avroClass))
  }

  def of[T <: IndexedRecord](schema: Schema): ExpressionEncoder[T] = {
    assert(toSqlType(schema).dataType.isInstanceOf[StructType])

    val avroClass = Option(ReflectData.get.getClass(schema))
      .map(_.asSubclass(classOf[SpecificRecord]))
      .getOrElse(classOf[GenericData.Record])
    val serializer = AvroTypeInference.serializerFor(avroClass, schema)
    val deserializer = AvroTypeInference.deserializerFor(schema)

    new ExpressionEncoder[T](
      toSqlType(schema).dataType.asInstanceOf[StructType],
      flat = false,
      serializer.flatten,
      deserializer,
      ClassTag[T](avroClass))
  }
}

/**
 * Utilities for providing Avro object serializers and deserializers
 */
private object AvroTypeInference {
  /**
   * Translates an Avro Schema type to a proper SQL DataType. The Java Objects that back data in
   * generated Generic and Specific records sometimes do not align with those suggested by Avro
   * ReflectData, so we infer the proper SQL DataType to serialize and deserialize based on
   * nullability and the wrapping Schema type.
   */
  private def inferExternalType(avroSchema: Schema): DataType = {
    toSqlType(avroSchema) match {
      // the non-nullable primitive types
      case SchemaType(BooleanType, false) => BooleanType
      case SchemaType(IntegerType, false) => IntegerType
      case SchemaType(LongType, false) =>
        if (avroSchema.getType == UNION) {
          ObjectType(classOf[java.lang.Number])
        } else {
          LongType
        }
      case SchemaType(FloatType, false) => FloatType
      case SchemaType(DoubleType, false) =>
        if (avroSchema.getType == UNION) {
          ObjectType(classOf[java.lang.Number])
        } else {
          DoubleType
        }
      // the nullable primitive types
      case SchemaType(BooleanType, true) => ObjectType(classOf[java.lang.Boolean])
      case SchemaType(IntegerType, true) => ObjectType(classOf[java.lang.Integer])
      case SchemaType(LongType, true) => ObjectType(classOf[java.lang.Long])
      case SchemaType(FloatType, true) => ObjectType(classOf[java.lang.Float])
      case SchemaType(DoubleType, true) => ObjectType(classOf[java.lang.Double])
      // the binary types
      case SchemaType(BinaryType, _) =>
        if (avroSchema.getType == FIXED) {
          Option(ReflectData.get.getClass(avroSchema))
            .map(ObjectType(_))
            .getOrElse(ObjectType(classOf[GenericData.Fixed]))
        } else {
          ObjectType(classOf[java.nio.ByteBuffer])
        }
      // the referenced types
      case SchemaType(ArrayType(_, _), _) =>
        ObjectType(classOf[java.util.List[Object]])
      case SchemaType(StringType, _) =>
        avroSchema.getType match {
          case ENUM =>
            Option(ReflectData.get.getClass(avroSchema))
              .map(ObjectType(_))
              .getOrElse(ObjectType(classOf[GenericData.EnumSymbol]))
          case _ =>
            ObjectType(classOf[CharSequence])
        }
      case SchemaType(StructType(_), _) =>
        Option(ReflectData.get.getClass(avroSchema))
          .map(ObjectType(_))
          .getOrElse(ObjectType(classOf[GenericData.Record]))
      case SchemaType(MapType(_, _, _), _) =>
        ObjectType(classOf[java.util.Map[Object, Object]])
    }
  }

  /**
   * Returns an expression that can be used to deserialize an InternalRow to an Avro object of
   * type `T` that implements IndexedRecord and is compatible with the given Schema
   */
  def deserializerFor[T <: IndexedRecord] (avroSchema: Schema): Expression = {
    deserializerFor(avroSchema, None)
  }

  private def deserializerFor(avroSchema: Schema, path: Option[Expression]): Expression = {
    def addToPath(part: String): Expression = path
      .map(p => UnresolvedExtractValue(p, Literal(part)))
      .getOrElse(UnresolvedAttribute(part))

    def getPath: Expression = path.getOrElse(
      GetColumnByOrdinal(0, inferExternalType(avroSchema)))

    avroSchema.getType match {
      case BOOLEAN =>
        NewInstance(
          classOf[java.lang.Boolean],
          getPath :: Nil,
          ObjectType(classOf[java.lang.Boolean]))
      case INT =>
        NewInstance(
          classOf[java.lang.Integer],
          getPath :: Nil,
          ObjectType(classOf[java.lang.Integer]))
      case LONG =>
        NewInstance(
          classOf[java.lang.Long],
          getPath :: Nil,
          ObjectType(classOf[java.lang.Long]))
      case FLOAT =>
        NewInstance(
          classOf[java.lang.Float],
          getPath :: Nil,
          ObjectType(classOf[java.lang.Float]))
      case DOUBLE =>
        NewInstance(
          classOf[java.lang.Double],
          getPath :: Nil,
          ObjectType(classOf[java.lang.Double]))

      case BYTES =>
        StaticInvoke(
          classOf[java.nio.ByteBuffer],
          ObjectType(classOf[java.nio.ByteBuffer]),
          "wrap",
          getPath :: Nil)
      case FIXED =>
        val fixedClass = Option(ReflectData.get.getClass(avroSchema))
          .getOrElse(classOf[GenericData.Fixed])
        if (fixedClass == classOf[GenericData.Fixed]) {
          NewInstance(
            fixedClass,
            Literal.fromObject(avroSchema, ObjectType(classOf[Schema])) ::
              getPath ::
              Nil,
            ObjectType(fixedClass))
        } else {
          NewInstance(
            fixedClass,
            getPath :: Nil,
            ObjectType(fixedClass))
        }

      case STRING =>
        Invoke(getPath, "toString", ObjectType(classOf[String]))

      case ENUM =>
        val enumClass = Option(ReflectData.get.getClass(avroSchema))
          .getOrElse(classOf[GenericData.EnumSymbol])
        if (enumClass == classOf[GenericData.EnumSymbol]) {
          NewInstance(
            enumClass,
            Literal.fromObject(avroSchema, ObjectType(classOf[Schema])) ::
              Invoke(getPath, "toString", ObjectType(classOf[String])) ::
              Nil,
            ObjectType(enumClass))
        } else {
          StaticInvoke(
            enumClass,
            ObjectType(enumClass),
            "valueOf",
            Invoke(getPath, "toString", ObjectType(classOf[String])) :: Nil)
        }

      case ARRAY =>
        val elementSchema = avroSchema.getElementType
        val elementType = toSqlType(elementSchema).dataType
        val array = Invoke(
          MapObjects(element =>
            deserializerFor(elementSchema, Some(element)),
            getPath,
            elementType),
          "array",
          ObjectType(classOf[Array[Any]]))

        StaticInvoke(
          classOf[java.util.Arrays],
          ObjectType(classOf[java.util.List[Object]]),
          "asList",
          array :: Nil)

      case MAP =>
        val valueSchema = avroSchema.getValueType
        val valueType = inferExternalType(valueSchema) match {
          case t if t == ObjectType(classOf[java.lang.CharSequence]) =>
            StringType
          case other => other
        }

        val keyData = Invoke(
          MapObjects(
            p => deserializerFor(Schema.create(STRING), Some(p)),
            Invoke(getPath, "keyArray", ArrayType(StringType)),
            StringType),
          "array",
          ObjectType(classOf[Array[Any]]))
        val valueData = Invoke(
          MapObjects(
            p => deserializerFor(valueSchema, Some(p)),
            Invoke(getPath, "valueArray", ArrayType(valueType)),
            valueType),
          "array",
          ObjectType(classOf[Array[Any]]))

        StaticInvoke(
          ArrayBasedMapData.getClass,
          ObjectType(classOf[JMap[_, _]]),
          "toJavaMap",
          keyData :: valueData :: Nil)

      case UNION =>
        val (resolvedSchema, _) = resolveUnionType(avroSchema)
        if (resolvedSchema.getType == RECORD &&
          avroSchema.getTypes.asScala.filterNot(_.getType == NULL).length > 1) {
          // A Union resolved to a record that originally had more than 1 type when filtered
          // of its nulls must be complex
          val bottom = Literal.create(null, ObjectType(classOf[Object])).asInstanceOf[Expression]

          resolvedSchema.getFields.foldLeft(bottom) { (tree: Expression, field: Schema.Field) =>
            val fieldValue = ObjectCast(
              deserializerFor(field.schema, Some(addToPath(field.name))),
              ObjectType(classOf[Object]))

            If(IsNull(fieldValue), tree, fieldValue)
          }
        } else {
          deserializerFor(resolvedSchema, path)
        }

      case RECORD =>
        val args = avroSchema.getFields.map { field =>
          val position = Literal(field.pos)
          val argument = deserializerFor(field.schema, Some(addToPath(field.name)))
          (position, argument)
        }.toList

        val recordClass = Option(ReflectData.get.getClass(avroSchema))
          .getOrElse(classOf[GenericData.Record])
        val newInstance = if (recordClass == classOf[GenericData.Record]) {
          NewInstance(
            recordClass,
            Literal.fromObject(avroSchema, ObjectType(classOf[Schema])) :: Nil,
            ObjectType(recordClass))
        } else {
          NewInstance(
            recordClass,
            Nil,
            ObjectType(recordClass))
        }

        val result = InitializeAvroObject(newInstance, args)

        if (path.nonEmpty) {
          If(IsNull(getPath),
            Literal.create(null, ObjectType(recordClass)),
            result)
        } else {
          result
        }

      case NULL =>
        /*
         * Encountering NULL at this level implies it was the type of a Field, which should never
         * be the case
         */
        throw new IncompatibleSchemaException("Null type should only be used in Union types")
    }
  }

  /**
   * Returns an expression that can be used to serialize an Avro object with a class of type `T`
   * that is compatible with the given Schema to an InternalRow
   */
  def serializerFor[T <: IndexedRecord](avroClass: Class[T], avroSchema: Schema):
  CreateNamedStruct = {
    val inputObject = BoundReference(0, ObjectType(avroClass), nullable = true)
    serializerFor(inputObject, avroSchema, topLevel = true).asInstanceOf[CreateNamedStruct]
  }

  def serializerFor(
    inputObject: Expression,
    avroSchema: Schema,
    topLevel: Boolean = false): Expression = {

    def toCatalystArray(inputObject: Expression, schema: Schema): Expression = {
      val elementType = inferExternalType(schema)

      if (elementType.isInstanceOf[ObjectType]) {
        MapObjects(element =>
          serializerFor(element, schema),
          Invoke(
            inputObject,
            "toArray",
            ObjectType(classOf[Array[Object]])),
          elementType)
      } else {
        NewInstance(
          classOf[GenericArrayData],
          inputObject :: Nil,
          dataType = ArrayType(elementType, containsNull = false))
      }
    }

    def toCatalystMap(inputObject: Expression, schema: Schema): Expression = {
      val valueSchema = schema.getValueType
      val valueType = inferExternalType(valueSchema)

      ExternalMapToCatalyst(
        inputObject,
        ObjectType(classOf[org.apache.avro.util.Utf8]),
        serializerFor(_, Schema.create(STRING)),
        valueType,
        serializerFor(_, valueSchema))
    }

    if (!inputObject.dataType.isInstanceOf[ObjectType]) {
      inputObject
    } else {
      avroSchema.getType match {
        case BOOLEAN =>
          Invoke(inputObject, "booleanValue", BooleanType)
        case INT =>
          Invoke(inputObject, "intValue", IntegerType)
        case LONG =>
          Invoke(inputObject, "longValue", LongType)
        case FLOAT =>
          Invoke(inputObject, "floatValue", FloatType)
        case DOUBLE =>
          Invoke(inputObject, "doubleValue", DoubleType)

        case BYTES =>
          Invoke(inputObject, "array", BinaryType)
        case FIXED =>
          Invoke(inputObject, "bytes", BinaryType)

        case STRING =>
          StaticInvoke(
            classOf[UTF8String],
            StringType,
            "fromString",
            Invoke(inputObject, "toString", ObjectType(classOf[java.lang.String])) :: Nil)

        case ENUM =>
          StaticInvoke(
            classOf[UTF8String],
            StringType,
            "fromString",
            Invoke(inputObject, "toString", ObjectType(classOf[java.lang.String])) :: Nil)

        case ARRAY =>
          val elementSchema = avroSchema.getElementType
          toCatalystArray(inputObject, elementSchema)

        case MAP =>
          toCatalystMap(inputObject, avroSchema)

        case UNION =>
          val unionWithoutNulls = Schema.createUnion(
            avroSchema.getTypes.asScala.filterNot(_.getType == NULL))
          val (resolvedSchema, nullable) = resolveUnionType(avroSchema)
          if (resolvedSchema.getType == RECORD && unionWithoutNulls.getTypes.length > 1) {
            // A Union resolved to a record that originally had more than 1 type when filtered
            // of its nulls must be complex
            val complexStruct = CreateNamedStruct(
              resolvedSchema.getFields.zipWithIndex.flatMap { case (field, index) =>
                val unionIndex = StaticInvoke(
                  classOf[GenericData],
                  IntegerType,
                  "get().resolveUnion",
                  Literal.fromObject(
                    unionWithoutNulls,
                    ObjectType(classOf[Schema])) :: inputObject :: Nil)

                val fieldValue = If(EqualTo(Literal(index), unionIndex),
                  serializerFor(
                    ObjectCast(
                      inputObject,
                      inferExternalType(field.schema())),
                    field.schema),
                  Literal.create(null, toSqlType(field.schema()).dataType))

                Literal(field.name) :: serializerFor(fieldValue, field.schema) :: Nil})

              complexStruct

          } else {
            if (nullable) {
              serializerFor(inputObject, resolvedSchema)
            } else {
              serializerFor(
                AssertNotNull(inputObject, Seq(avroSchema.getTypes.toString)),
                resolvedSchema)
            }
          }

        case RECORD =>
          val createStruct = CreateNamedStruct(
            avroSchema.getFields.flatMap { field =>
              val fieldValue = Invoke(
                inputObject,
                "get",
                inferExternalType(field.schema),
                Literal(field.pos) :: Nil)
              Literal(field.name) :: serializerFor(fieldValue, field.schema) :: Nil})
          if (topLevel) {
            createStruct
          } else {
            If(IsNull(inputObject),
              Literal.create(null, createStruct.dataType),
              createStruct)
          }

        case NULL =>
          /*
           * Encountering NULL at this level implies it was the type of a Field, which should never
           * be the case
           */
          throw new IncompatibleSchemaException("Null type should only be used in Union types")
      }
    }
  }

  /**
   * Initializes an Avro Record instance (that implements the IndexedRecord interface) by calling
   * the `put` method on a the Record instance with the provided position and value arguments
   * @param objectInstance an expression that will evaluate to the Record instance
   * @param args a sequence of expression pairs that will respectively evaluate to the index of
   *             the record in which to insert, and the argument value to insert
   */
  private case class InitializeAvroObject(
    objectInstance: Expression,
    args: List[(Expression, Expression)]) extends Expression with NonSQLExpression {

    override def nullable: Boolean = objectInstance.nullable
    override def children: Seq[Expression] = objectInstance +: args.map { case (_, v) => v }
    override def dataType: DataType = objectInstance.dataType

    override def eval(input: InternalRow): Any =
      throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

    override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
      val instanceGen = objectInstance.genCode(ctx)

      val avroInstance = ctx.freshName("avroObject")
      val avroInstanceJavaType = ctx.javaType(objectInstance.dataType)
      ctx.addMutableState(avroInstanceJavaType, avroInstance, "")

      val initialize = args.map {
        case (posExpr, argExpr) =>
          val posGen = posExpr.genCode(ctx)
          val argGen = argExpr.genCode(ctx)
          s"""
            ${posGen.code}
            ${argGen.code}
            $avroInstance.put(${posGen.value}, ${argGen.value});
          """
      }

      val initExpressions = ctx.splitExpressions(ctx.INPUT_ROW, initialize)
      val code =
        s"""
          ${instanceGen.code}
          $avroInstance = ${instanceGen.value};
          if (!${instanceGen.isNull}) {
            $initExpressions
          }
        """
      ev.copy(code = code, isNull = instanceGen.isNull, value = instanceGen.value)
    }
  }

  /**
   * Casts an expression to another object.
   *
   * @param value The value to cast
   * @param resultType The type the value should be cast to.
   */
  private case class ObjectCast(
    value : Expression,
    resultType: DataType) extends Expression with NonSQLExpression {

    override def nullable: Boolean = value.nullable
    override def dataType: DataType = resultType
    override def children: Seq[Expression] = value :: Nil

    override def eval(input: InternalRow): Any =
      throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {

      val javaType = ctx.javaType(resultType)
      val obj = value.genCode(ctx)

      val code = s"""
         ${obj.code}
         final $javaType ${ev.value} = ($javaType) ${obj.value};
       """

      ev.copy(code = code, isNull = obj.isNull)
    }
  }
}
