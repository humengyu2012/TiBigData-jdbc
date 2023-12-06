package io.tidb.bigdata.jdbc.core;

import java.util.Map;
import java.util.Objects;

public class TiDBCoreConfig {

  public static final String DATABASE_URL = "tidb.database.url";
  public static final String USERNAME = "tidb.username";
  public static final String PASSWORD = "tidb.password";
  public static final String TIDB_SNAPSHOT = "tidb_snapshot";

  private final String databaseUrl;
  private final String username;
  private final String password;

  public TiDBCoreConfig(String databaseUrl, String username, String password) {
    this.databaseUrl = Objects.requireNonNull(databaseUrl, "database url is null");
    this.username = Objects.requireNonNull(username, "username is null");
    this.password = Objects.requireNonNull(password, "password is null");
  }

  public String getDatabaseUrl() {
    return databaseUrl;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public static TiDBCoreConfig fromProperties(Map<String, String> properties) {
    return new TiDBCoreConfig(
        properties.get(DATABASE_URL), properties.get(USERNAME), properties.get(PASSWORD));
  }
}
