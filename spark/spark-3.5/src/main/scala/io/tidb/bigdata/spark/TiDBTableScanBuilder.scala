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

import io.tidb.bigdata.jdbc.core.TiDBJdbcUtils
import org.apache.spark.Partition
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.datasources.jdbc._
import org.apache.spark.sql.execution.datasources.v2.jdbc._
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

case class TiDBTableScanBuilder(ident: Identifier, jdbcScanBuilder: JDBCScanBuilder)
  extends ScanBuilder
    with SupportsPushDownV2Filters
    with SupportsPushDownRequiredColumns
    with SupportsPushDownAggregates
    with SupportsPushDownLimit
    with SupportsPushDownOffset
    with SupportsPushDownTopN {
  override def build(): Scan = {
    val scan = jdbcScanBuilder.build()

    TiDBTableScan(jdbcScanBuilder.jdbcOptions, tso(), partitions(), scan.prunedSchema, scan.pushedPredicates, scan.pushedAggregateColumn,
      scan.groupByColumns, scan.pushedLimit, scan.sortOrders, scan.pushedOffset)
  }

  private def tso(): Long = {
    JdbcUtils.withConnection(jdbcScanBuilder.jdbcOptions) { conn =>
      TiDBJdbcUtils.queryVersion(conn)
    }
  }

  private def partitions(): Array[Partition] = {
    JdbcUtils.withConnection(jdbcScanBuilder.jdbcOptions) { conn =>
      val partitionBuilder = mutable.ArrayBuilder.make[Partition]
      JdbcUtils.executeQuery(conn, jdbcScanBuilder.jdbcOptions, s"SHOW TABLE `${ident.name}` SPLITS") { rs =>
        var index: Int = 0;
        while (rs.next()) {
          partitionBuilder += TiDBTablePartition(
            rs.getString("START"),
            rs.getString("END"),
            rs.getString("PARTITION"),
            index,
          )
          index += 1
        }
      }
      partitionBuilder.result
    }
  }

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = jdbcScanBuilder.pushPredicates(predicates)

  override def pushedPredicates(): Array[Predicate] = jdbcScanBuilder.pushedPredicates()

  override def pruneColumns(structType: StructType): Unit = jdbcScanBuilder.pruneColumns(structType)

  override def pushAggregation(aggregation: Aggregation): Boolean = jdbcScanBuilder.pushAggregation(aggregation)

  override def pushLimit(i: Int): Boolean = jdbcScanBuilder.pushLimit(i)

  override def pushOffset(i: Int): Boolean = jdbcScanBuilder.pushOffset(i)

  override def pushTopN(sortOrders: Array[SortOrder], i: Int): Boolean = jdbcScanBuilder.pushTopN(sortOrders, i)

  override def isPartiallyPushed: Boolean = jdbcScanBuilder.isPartiallyPushed()
}