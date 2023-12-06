package io.tidb.bigdata.jdbc.core;

import java.io.Serializable;

public class TiDBJdbcSplit implements Serializable {

  private final String databaseName;
  private final String tableName;
  private final String startKey;
  private final String endKey;
  private final long version;

  public TiDBJdbcSplit(
      String databaseName, String tableName, String startKey, String endKey, long version) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.startKey = startKey;
    this.endKey = endKey;
    this.version = version;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getStartKey() {
    return startKey;
  }

  public String getEndKey() {
    return endKey;
  }

  public long getVersion() {
    return version;
  }

  @Override
  public String toString() {
    return "TiDBJdbcSplit{"
        + "databaseName='"
        + databaseName
        + '\''
        + ", tableName='"
        + tableName
        + '\''
        + ", startKey='"
        + startKey
        + '\''
        + ", endKey='"
        + endKey
        + '\''
        + ", version="
        + version
        + '}';
  }
}
