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

import org.apache.spark.sql.types.DataType

/*
 * Schema namespace naming strategies for hierarchical types such as struct, maps and arrays
 */
private[avro] object SchemaNsNaming {
  def fromName(name: String): SchemaNsNaming = FlatSchemaNsNaming(name)

  case class HierarchicalSchemaNsNaming(namespace: String, fieldName: String)
    extends SchemaNsNaming {

    override val currentNamespace: String =
      if (namespace == null) fieldName else s"$namespace.$fieldName"
  }

  case class StructSchemaNsNaming(currentNamespace: String, structFieldName: String)
    extends SchemaNsNaming

  case class FlatSchemaNsNaming(currentNamespace: String) extends SchemaNsNaming

}

private[avro] sealed trait SchemaNsNaming {
  import SchemaNsNaming._

  def currentNamespace: String

  def structFieldNaming(fieldName: String): SchemaNsNaming =
    HierarchicalSchemaNsNaming(currentNamespace, fieldName)

  def arrayFieldNaming(fieldName: String, valueType: DataType): SchemaNsNaming = this

  def mapFieldNaming(fieldName: String, valueType: DataType): SchemaNsNaming = this
}

