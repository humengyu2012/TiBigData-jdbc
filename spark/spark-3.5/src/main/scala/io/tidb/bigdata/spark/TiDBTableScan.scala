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

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.V1Scan
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.execution.datasources.v2.jdbc.TiDBTableRDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

case class TiDBTableScan(
                          jdbcOptions: JDBCOptions,
                          version: Long,
                          parts: Array[Partition],
                          prunedSchema: StructType,
                          pushedPredicates: Array[Predicate],
                          pushedAggregateColumn: Array[String] = Array(),
                          groupByColumns: Option[Array[String]],
                          pushedLimit: Int,
                          sortOrders: Array[String],
                          pushedOffset: Int) extends V1Scan {

  override def readSchema(): StructType = prunedSchema

  override def toV1TableScan[T <: BaseRelation with TableScan](context: SQLContext): T = {
    new BaseRelation with TableScan {
      override def sqlContext: SQLContext = context

      override def schema: StructType = prunedSchema

      override def needConversion: Boolean = false

      override def buildScan(): RDD[Row] = {
        val columnList = if (groupByColumns.isEmpty) {
          prunedSchema.map(_.name).toArray
        } else {
          pushedAggregateColumn
        }
        val session = SparkSession.active
        TiDBTableRDD.scanTable(
          session.sparkContext,
          columnList,
          pushedPredicates,
          parts,
          jdbcOptions,
          prunedSchema,
          groupByColumns,
          pushedLimit,
          sortOrders,
          pushedOffset,
          version).asInstanceOf[RDD[Row]]
      }
    }.asInstanceOf[T]
  }

  override def description(): String = {
    val (aggString, groupByString) = if (groupByColumns.nonEmpty) {
      val groupByColumnsLength = groupByColumns.get.length
      (seqToString(pushedAggregateColumn.drop(groupByColumnsLength)),
        seqToString(pushedAggregateColumn.take(groupByColumnsLength)))
    } else {
      ("[]", "[]")
    }
    super.description() + ", prunedSchema: " + seqToString(prunedSchema) +
      ", PushedPredicates: " + seqToString(pushedPredicates) +
      ", PushedAggregates: " + aggString + ", PushedGroupBy: " + groupByString
  }

  private def seqToString(seq: Seq[Any]): String = seq.mkString("[", ", ", "]")
}