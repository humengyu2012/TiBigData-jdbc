package io.tidb.bigdata.jdbc.core;

import io.tidb.bigdata.jdbc.core.TiDBColumn.JavaType;
import io.tidb.bigdata.jdbc.core.TiDBColumn.TiDBType;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class TiDBJdbcUtils {

  public static final String QUERY_TS_SQL = "SELECT @@tidb_current_ts";
  public static final String SET_TS_SQL_FORMAT = "SET @@tidb_snapshot=%s";
  public static final String QUERY_SPLIT_FORMAT = "SHOW TABLE `%s`.`%s` SPLITS";
  public static final String QUERY_LIMIT_1_FORMAT = "SELECT * FROM `%s`.`%s` LIMIT 1";

  public static Connection establishNewConnection(TiDBCoreConfig config) throws SQLException {
    return DriverManager.getConnection(
        config.getDatabaseUrl(), config.getUsername(), config.getPassword());
  }

  public static List<TiDBJdbcSplit> querySplits(
      Connection connection, String database, String table, long version, boolean shuffle)
      throws SQLException {
    List<TiDBJdbcSplit> splits = new ArrayList<>();
    String sql = String.format(QUERY_SPLIT_FORMAT, database, table);
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      while (resultSet.next()) {
        String startKey = resultSet.getString("START");
        String endKey = resultSet.getString("END");
        splits.add(new TiDBJdbcSplit(database, table, startKey, endKey, version));
      }
    }
    if (shuffle) {
      Collections.shuffle(splits);
    }
    return splits;
  }

  public static long queryVersion(Connection connection) throws SQLException {
    boolean autoCommit = connection.getAutoCommit();
    connection.setAutoCommit(false);
    try (Statement statement = connection.createStatement()) {
      statement.execute("BEGIN");
      try (ResultSet resultSet = statement.executeQuery(QUERY_TS_SQL)) {
        resultSet.next();
        return resultSet.getLong(1);
      } finally {
        statement.execute("ROLLBACK");
      }
    } finally {
      connection.setAutoCommit(autoCommit);
    }
  }

  public static long parseVersionFromString(String s) {
    try {
      return Long.parseLong(s);
    } catch (NumberFormatException e) {
      return Timestamp.valueOf(s).getTime() << 18;
    }
  }

  public static List<TiDBColumn> queryTiDBColumns(
      Connection connection, String database, String table) throws SQLException {
    List<TiDBColumn> types = new ArrayList<>();
    String sql = String.format(QUERY_LIMIT_1_FORMAT, database, table);
    try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
      ResultSetMetaData metaData = preparedStatement.getMetaData();
      int columnCount = metaData.getColumnCount();
      for (int i = 1; i <= columnCount; i++) {
        String columnName = metaData.getColumnName(i);
        int precision = metaData.getPrecision(i);
        int scale = metaData.getScale(i);
        String columnClassName = metaData.getColumnClassName(i);
        JavaType javaType = JavaType.fromClassName(columnClassName);
        boolean nullable = metaData.isNullable(i) == ResultSetMetaData.columnNullable;
        types.add(new TiDBColumn(columnName, new TiDBType(javaType, precision, scale, nullable)));
      }
    }
    return types;
  }

  public static String getQuerySql(TiDBJdbcSplit split, List<String> columns) {
    String columnsString =
        columns.stream().map(column -> "`" + column + "`").collect(Collectors.joining(","));
    return String.format(
        "SELECT %s FROM `%s`.`%s` TABLESPLIT('%s', '%s')",
        columnsString,
        split.getDatabaseName(),
        split.getTableName(),
        split.getStartKey(),
        split.getEndKey());
  }

  public static TiDBJdbcRecordCursor scan(
      Connection connection, List<TiDBJdbcSplit> splits, List<String> columns) throws SQLException {
    Statement statement = connection.createStatement();
    List<Supplier<ResultSet>> resultSets =
        splits.stream()
            .map(
                split ->
                    (Supplier<ResultSet>)
                        () -> {
                          try {
                            String querySql = getQuerySql(split, columns);
                            statement.execute(
                                String.format(TiDBJdbcUtils.SET_TS_SQL_FORMAT, split.getVersion()));
                            return statement.executeQuery(querySql);
                          } catch (SQLException e) {
                            throw new IllegalStateException("Can not get resultSet", e);
                          }
                        })
            .collect(Collectors.toList());

    return new TiDBJdbcRecordCursor(statement, resultSets, columns);
  }
}
