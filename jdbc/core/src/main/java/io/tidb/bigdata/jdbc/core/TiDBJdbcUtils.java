package io.tidb.bigdata.jdbc.core;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class TiDBJdbcUtils {

  private static final String QUERY_TS_SQL = "SELECT @@tidb_current_ts";
  private static final String QUERY_SPLIT_FORMAT = "SHOW TABLE `%s`.`%s` SPLITS";

  public static List<TiDBJdbcSplit> querySplits(
      Connection connection, String database, String table, long version) throws SQLException {
    List<TiDBJdbcSplit> splits = new ArrayList<>();
    String sql = String.format(QUERY_SPLIT_FORMAT, database, table);
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      while (resultSet.next()) {
        String startKey = resultSet.getString("START");
        String endKey = resultSet.getString("END");
        splits.add(new TiDBJdbcSplit(startKey, endKey, version));
      }
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
}
