package io.tidb.bigdata.jdbc.core;

import java.io.Serializable;

public class TiDBJdbcSplit implements Serializable {

  private final String startKey;
  private final String endKey;
  private final long version;

  public TiDBJdbcSplit(String startKey, String endKey, long version) {
    this.startKey = startKey;
    this.endKey = endKey;
    this.version = version;
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
        + "startKey='"
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
