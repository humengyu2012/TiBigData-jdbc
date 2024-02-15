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

package org.apache.spark.sql.execution.datasources.v2.jdbc

import io.tidb.bigdata.jdbc.core.TiDBJdbcUtils
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCPartition, JdbcUtils}
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCContext.closeAutoCloseable
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcSQLQueryBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.CompletionIterator
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}

import java.sql.{Connection, ResultSet, Statement}
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

case class TiDBTablePartition(start: String, end: String, partition: String, idx: Int) extends Partition {
  override def index: Int = idx
}

object TiDBTableRDD extends Logging {
  def scanTable(
                 sc: SparkContext,
                 requiredColumns: Array[String],
                 predicates: Array[Predicate],
                 parts: Array[Partition],
                 options: JDBCOptions,
                 outputSchema: StructType,
                 groupByColumns: Option[Array[String]] = None,
                 limit: Int = 0,
                 sortOrders: Array[String] = Array.empty[String],
                 offset: Int = 0,
                 version: Long = 0): RDD[InternalRow] = {
    val url = options.url
    val dialect = JdbcDialects.get(url)
    val quotedColumns = if (groupByColumns.isEmpty) {
      requiredColumns.map(colName => dialect.quoteIdentifier(colName))
    } else {
      // these are already quoted in JDBCScanBuilder
      requiredColumns
    }
    new TiDBTableRDD(
      sc,
      dialect.createConnectionFactory(options),
      outputSchema,
      quotedColumns,
      predicates,
      parts,
      url,
      options,
      groupByColumns,
      limit,
      sortOrders,
      offset,
      version)
  }
}

class TiDBTableRDD(
                    sc: SparkContext,
                    getConnection: Int => Connection,
                    schema: StructType,
                    columns: Array[String],
                    predicates: Array[Predicate],
                    partitions: Array[Partition],
                    url: String,
                    options: JDBCOptions,
                    groupByColumns: Option[Array[String]],
                    limit: Int,
                    sortOrders: Array[String],
                    offset: Int,
                    version: Long)
  extends RDD[InternalRow](sc, Nil) {

  /**
   * Retrieve the list of partitions corresponding to this RDD.
   */
  override def getPartitions: Array[Partition] = partitions

  /**
   * Runs the SQL query against the JDBC driver.
   *
   */
  override def compute(thePart: Partition, context: TaskContext): Iterator[InternalRow] = {
    val jdbcContext = new JDBCContext(options)

    context.addTaskCompletionListener[Unit] { context => jdbcContext.close() }

    val inputMetrics = context.taskMetrics().inputMetrics
    val part = thePart.asInstanceOf[TiDBTablePartition]
    jdbcContext.init(getConnection(part.idx))
    val dialect = JdbcDialects.get(url)
    dialect.beforeFetch(jdbcContext.connection, options.asProperties.asScala.toMap)

    // Executes session init statement
    options.sessionInitStatement match {
      case Some(sql) =>
        logInfo(s"Executing ${JDBCOptions.JDBC_SESSION_INIT_STATEMENT} : $sql")
        jdbcContext.execute(sql)
      case None =>
    }

    jdbcContext.snapshot(version)

    var builder = new TiDBSQLQueryBuilder(dialect, options, part)
      .withColumns(columns)
      .withPredicates(predicates, JDBCPartition(null, part.index))
      .withSortOrders(sortOrders)
      .withLimit(limit)
      .withOffset(offset)

    groupByColumns.foreach { groupByKeys =>
      builder = builder.withGroupByColumns(groupByKeys)
    }

    val rowsIterator =
      JdbcUtils.resultSetToSparkInternalRows(jdbcContext.executeQuery(builder.build()), dialect, schema, inputMetrics)

    CompletionIterator[InternalRow, Iterator[InternalRow]](
      new InterruptibleIterator(context, rowsIterator), () => jdbcContext.close())
  }
}

class TiDBSQLQueryBuilder(dialect: JdbcDialect, options: JDBCOptions, part: TiDBTablePartition)
  extends JdbcSQLQueryBuilder(dialect, options) {

  override def build(): String = {
    val limitOrOffsetStmt = if (limit > 0) {
      if (offset > 0) {
        s"LIMIT $offset, $limit"
      } else {
        dialect.getLimitClause(limit)
      }
    } else if (offset > 0) {
      s"LIMIT $offset, 18446744073709551615"
    } else {
      ""
    }

    options.prepareQuery +
      s"SELECT $columnList FROM ${options.tableOrQuery} $splitClause" +
      s" $whereClause $groupByClause $orderByClause $limitOrOffsetStmt"
  }

  private def splitClause: String = {
    var tableSplit = s"TABLESPLIT('${part.start}', '${part.end}')"
    if (part.partition != null && part.partition.nonEmpty) {
      s" PARTITION(`${part.partition}`) ${tableSplit}"
    } else {
      tableSplit
    }
  }
}

class JDBCContext(options: JDBCOptions) extends Logging {
  var closed: Boolean = false
  var resultSet: ResultSet = null;
  var statement: Statement = null;
  var connection: Connection = null;

  def init(conn: Connection): Unit = {
    connection = conn
    connection.setAutoCommit(false)
    statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    statement.setQueryTimeout(options.queryTimeout)
    statement.setFetchSize(options.fetchSize)
  }

  def close(): Unit = {
    if (closed) {
      return
    }
    resultSet = closeAutoCloseable(resultSet)
    statement = closeAutoCloseable(statement)
    connection = closeAutoCloseable(connection, (c: Connection) => {
      if (!c.isClosed && !c.getAutoCommit) {
        try {
          c.commit()
        } catch {
          case NonFatal(e) => logWarning("Exception committing transaction", e)
        }
      }
      c
    })
    closed = true
  }

  def snapshot(version: Long): Unit = {
    connection.commit()
    TiDBJdbcUtils.setSnapshot(connection, version)
  }

  def execute(sql: String): Unit = {
    statement.execute(sql)
  }

  def executeQuery(sql: String): ResultSet = {
    statement.executeQuery(sql)
  }
}

object JDBCContext extends Logging {
  def closeAutoCloseable[T <: AutoCloseable](closable: T, prelude: (T) => T = noopPrelude[T] _): T = {
    if (closable != null) {
      try {
        prelude(closable).close()
      } catch {
        case e: Exception => logWarning("Exception closing " + closable, e)
      }
    }
    null.asInstanceOf[T]
  }

  private def noopPrelude[T <: AutoCloseable](closable: T): T = closable
}