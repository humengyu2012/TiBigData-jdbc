/*
 * Copyright 2020 TiDB Project Authors.
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

package io.tidb.bigdata.jdbc.core;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.tikv.shade.com.google.common.base.MoreObjects.toStringHelper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TiDBJdbcClient implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(TiDBJdbcClient.class);

  public static final String TIDB_ROW_ID_COLUMN_NAME = "_tidb_rowid";

  private static final Set<String> BUILD_IN_DATABASES =
      new HashSet<>(
          Arrays.asList("information_schema", "metrics_schema", "performance_schema", "mysql"));

  private final TiDBCoreConfig config;

  private TiDBJdbcClient(TiDBCoreConfig config) {
    this.config = requireNonNull(config, "config is null");
  }

  public List<String> getSchemaNames() {
    String sql = "SHOW DATABASES";
    try (Connection connection = establishNewConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);
      List<String> databaseNames = new ArrayList<>();
      while (resultSet.next()) {
        String databaseName = resultSet.getString(1).toLowerCase();
        databaseNames.add(databaseName);
      }
      return databaseNames;
    } catch (Exception e) {
      LOG.error("Execute sql {} fail", sql, e);
      throw new IllegalStateException(e);
    }
  }

  public List<String> getTableNames(String schema) {
    String sql = "SHOW TABLES";
    requireNonNull(schema, "schema is null");
    try (Connection connection = establishNewConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + schema);
      ResultSet resultSet = statement.executeQuery(sql);
      List<String> tableNames = new ArrayList<>();
      while (resultSet.next()) {
        tableNames.add(resultSet.getString(1).toLowerCase());
      }
      return tableNames;
    } catch (Exception e) {
      LOG.error("Execute sql {} fail", sql, e);
      throw new IllegalStateException(e);
    }
  }

  public List<TiDBColumn> getAllColumns(String database, String table) {
    try {
      return TiDBJdbcUtils.queryTiDBColumns(establishNewConnection(), database, table);
    } catch (SQLException e) {
      throw new IllegalStateException(
          String.format("Can not query columns for table: `%s`.`%s`", database, table), e);
    }
  }

  public List<TiDBColumn> getColumnsByNames(String database, String table, List<String> columns) {
    return selectColumns(getAllColumns(database, table), columns);
  }

  private static List<TiDBColumn> selectColumns(
      List<TiDBColumn> allColumns, Stream<String> columns) {
    final Map<String, TiDBColumn> columnsMap =
        allColumns.stream().collect(Collectors.toMap(TiDBColumn::getName, Function.identity()));
    return columns
        .map(
            column ->
                Objects.requireNonNull(
                    columnsMap.get(column), "Column `" + column + "` does not exist"))
        .collect(Collectors.toList());
  }

  public static List<TiDBColumn> selectColumns(List<TiDBColumn> allColumns, List<String> columns) {
    return selectColumns(allColumns, columns.stream());
  }

  public void sqlUpdate(String... sqls) {
    try (Connection connection = establishNewConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : sqls) {
        LOG.info("Sql update: " + sql);
        statement.executeUpdate(sql);
      }
    } catch (Exception e) {
      LOG.error("Execute sql fail", e);
      throw new IllegalStateException(e);
    }
  }

  public int queryTableCount(String databaseName, String tableName) {
    try (Connection connection = establishNewConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                format("SELECT COUNT(*) as c FROM `%s`.`%s`", databaseName, tableName))) {
      resultSet.next();
      return resultSet.getInt("c");
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  public int queryIndexCount(String databaseName, String tableName, String indexName) {
    try (Connection connection = establishNewConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                format(
                    "SELECT COUNT(`%s`) as c FROM `%s`.`%s`",
                    indexName, databaseName, tableName))) {
      resultSet.next();
      return resultSet.getInt("c");
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  public boolean databaseExists(String databaseName) {
    return getSchemaNames().contains(requireNonNull(databaseName));
  }

  public boolean tableExists(String databaseName, String tableName) {
    return databaseExists(requireNonNull(databaseName))
        && getTableNames(databaseName).contains(requireNonNull(tableName));
  }

  public Connection getJdbcConnection() throws SQLException {
    return establishNewConnection();
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("config", config).toString();
  }

  public long getSnapshotVersion() {
    try (Connection connection = establishNewConnection()) {
      return TiDBJdbcUtils.queryVersion(connection);
    } catch (Exception e) {
      throw new IllegalStateException("Can not query version", e);
    }
  }

  /**
   * Most of the time, if the user does not explicitly specify a snapshot time, we don't need to
   * fetch the latest snapshot. A "relatively recent" snapshot is sufficient, which is crucial for
   * merging CDC data.
   *
   * @return Current version with zero logical time.
   */
  public long getApproximateSnapshotVersion() {
    long version = getSnapshotVersion();
    return (version >> 18) << 18;
  }

  public List<TiDBJdbcSplit> getSplits(
      String database, String table, long version, boolean shuffle) {
    try {
      List<TiDBJdbcSplit> splits =
          TiDBJdbcUtils.querySplits(establishNewConnection(), database, table, version, shuffle);
      LOG.info("The number of split for table `{}`.`{}` is {}", database, table, splits.size());
      return Collections.unmodifiableList(splits);
    } catch (SQLException e) {
      throw new IllegalStateException("Can not get splits", e);
    }
  }

  public TiDBJdbcRecordCursor scan(
      List<TiDBJdbcSplit> splits, List<TiDBColumn> columns, Integer limit, String whereCondition) {
    try {
      return TiDBJdbcUtils.scan(establishNewConnection(), splits, columns, limit, whereCondition);
    } catch (SQLException e) {
      throw new IllegalStateException("Can not scan splits", e);
    }
  }

  @Override
  public void close() {}

  private Connection establishNewConnection() throws SQLException {
    return TiDBJdbcUtils.establishNewConnection(config);
  }

  private static void close(AutoCloseable... resources) {
    for (AutoCloseable resource : resources) {
      try {
        resource.close();
      } catch (Exception e) {
        LOG.warn("Can not close resource", e);
      }
    }
  }

  public static TiDBJdbcClient create(TiDBCoreConfig config) {
    return new TiDBJdbcClient(config);
  }
}
