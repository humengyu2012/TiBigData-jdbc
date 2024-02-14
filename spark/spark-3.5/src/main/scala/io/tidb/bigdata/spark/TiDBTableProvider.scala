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

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCRDD, JdbcRelationProvider, JdbcUtils}
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTable
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import java.util
import scala.collection.JavaConverters._

class TiDBTableProvider
  extends TableProvider
    with DataSourceRegister
    with CreatableRelationProvider
    with RelationProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    getTable(null, Array.empty[Transform], options.asCaseSensitiveMap()).schema()
  }

  override def getTable(
                         schema: StructType,
                         partitioning: Array[Transform],
                         properties: util.Map[String, String]): Table = {
    val options = TiDBOptions(CaseInsensitiveMap(properties.asScala.toMap))
    val namespaces: Array[String] = JdbcUtils.withConnection(options) { conn =>
      var database: String = null
      JdbcUtils.executeQuery(conn, options, "SELECT DATABASE()") { rs =>
        rs.next()
        database = rs.getString(1)
      }
      if (database == null) {
        Array[String]()
      } else {
        Array[String](database)
      }
    }
    TiDBTable(JDBCTable(Identifier.of(namespaces, options.tableOrQuery), JDBCRDD.resolveTable(options), options))
  }

  override def shortName(): String = "tidb"

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    new JdbcRelationProvider().createRelation(sqlContext, mode, TiDBOptions.jdbcOptions(parameters), data)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    new JdbcRelationProvider().createRelation(sqlContext, TiDBOptions.jdbcOptions(parameters))
  }
}