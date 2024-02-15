/*
 * Copyright 2024 TiDB Project Authors.
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

package io.tidb.bigdata.spark

import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.index.{SupportsIndex, TableIndex}
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.v2.jdbc._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

case class TiDBTable(jdbcTable: JDBCTable) extends Table with SupportsRead with SupportsWrite with SupportsIndex {
  override def name(): String = jdbcTable.name

  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(BATCH_READ, V1_BATCH_WRITE, TRUNCATE)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): TiDBTableScanBuilder = {
    TiDBTableScanBuilder(jdbcTable.ident, jdbcTable.newScanBuilder(options))
  }

  override def schema(): StructType = jdbcTable.schema

  override def newWriteBuilder(logicalWriteInfo: LogicalWriteInfo): WriteBuilder = jdbcTable.newWriteBuilder(logicalWriteInfo)

  override def createIndex(s: String, namedReferences: Array[NamedReference], map: util.Map[NamedReference, util.Map[String, String]], map1: util.Map[String, String]): Unit = jdbcTable.createIndex(s, namedReferences, map, map1)

  override def dropIndex(s: String): Unit = jdbcTable.dropIndex(s)

  override def indexExists(s: String): Boolean = jdbcTable.indexExists(s)

  override def listIndexes(): Array[TableIndex] = jdbcTable.listIndexes()
}