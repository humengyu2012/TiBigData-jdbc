/*
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
package io.tidb.bigdata.prestodb;

import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.mysql.jdbc.SQLError.SQL_STATE_ER_TABLE_EXISTS_ERROR;
import static com.mysql.jdbc.SQLError.SQL_STATE_SYNTAX_ERROR;
import static java.util.Locale.ENGLISH;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.ConnectionFactory;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcIdentity;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTableLayoutHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableSet;
import com.mysql.jdbc.Driver;
import com.mysql.jdbc.Statement;
import io.tidb.bigdata.jdbc.core.TiDBJdbcUtils;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import javax.inject.Inject;

public class TiDBJdbcClient extends BaseJdbcClient {

  private static final Logger log = Logger.get(TiDBJdbcClient.class);

  @Inject
  public TiDBJdbcClient(JdbcConnectorId connectorId, BaseJdbcConfig config, TiDBConfig tiDBConfig)
      throws SQLException {
    super(connectorId, config, "`", connectionFactory(config, tiDBConfig));
  }

  private static ConnectionFactory connectionFactory(BaseJdbcConfig config, TiDBConfig tiDBConfig)
      throws SQLException {
    Properties connectionProperties = basicConnectionProperties(config);
    connectionProperties.setProperty("useInformationSchema", "true");
    connectionProperties.setProperty("nullCatalogMeansCurrent", "false");
    connectionProperties.setProperty("useUnicode", "true");
    connectionProperties.setProperty("characterEncoding", "utf8");
    connectionProperties.setProperty("tinyInt1isBit", "false");
    if (tiDBConfig.isAutoReconnect()) {
      connectionProperties.setProperty(
          "autoReconnect", String.valueOf(tiDBConfig.isAutoReconnect()));
      connectionProperties.setProperty(
          "maxReconnects", String.valueOf(tiDBConfig.getMaxReconnects()));
    }
    if (tiDBConfig.getConnectionTimeout() != null) {
      connectionProperties.setProperty(
          "connectTimeout", String.valueOf(tiDBConfig.getConnectionTimeout().toMillis()));
    }

    return new DriverConnectionFactory(
        new Driver(),
        config.getConnectionUrl(),
        Optional.ofNullable(config.getUserCredentialName()),
        Optional.ofNullable(config.getPasswordCredentialName()),
        connectionProperties);
  }

  @Override
  protected Collection<String> listSchemas(Connection connection) {
    // for MySQL, we need to list catalogs instead of schemas
    try (ResultSet resultSet = connection.getMetaData().getCatalogs()) {
      ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
      while (resultSet.next()) {
        String schemaName = resultSet.getString("TABLE_CAT");
        // skip internal schemas
        if (!schemaName.equalsIgnoreCase("information_schema")
            && !schemaName.equalsIgnoreCase("mysql")) {
          schemaNames.add(schemaName);
        }
      }
      return schemaNames.build();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void abortReadConnection(Connection connection) throws SQLException {
    // Abort connection before closing. Without this, the MySQL driver
    // attempts to drain the connection by reading all the results.
    connection.abort(directExecutor());
  }

  @Override
  public PreparedStatement getPreparedStatement(
      ConnectorSession session, Connection connection, String sql) throws SQLException {
    PreparedStatement statement = connection.prepareStatement(sql);
    if (statement.isWrapperFor(Statement.class)) {
      statement.unwrap(Statement.class).enableStreamingResults();
    }
    return statement;
  }

  @Override
  protected ResultSet getTables(
      Connection connection, Optional<String> schemaName, Optional<String> tableName)
      throws SQLException {
    // MySQL maps their "database" to SQL catalogs and does not have schemas
    DatabaseMetaData metadata = connection.getMetaData();
    Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
    return metadata.getTables(
        schemaName.orElse(null),
        null,
        escapeNamePattern(tableName, escape).orElse(null),
        new String[]{"TABLE", "VIEW"});
  }

  @Override
  protected String getTableSchemaName(ResultSet resultSet) throws SQLException {
    // MySQL uses catalogs instead of schemas
    return resultSet.getString("TABLE_CAT");
  }

  @Override
  protected String toSqlType(Type type) {
    if (REAL.equals(type)) {
      return "float";
    }
    if (TIME_WITH_TIME_ZONE.equals(type) || TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
      throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }
    if (TIMESTAMP.equals(type)) {
      return "datetime";
    }
    if (VARBINARY.equals(type)) {
      return "mediumblob";
    }
    if (isVarcharType(type)) {
      VarcharType varcharType = (VarcharType) type;
      if (varcharType.isUnbounded()) {
        return "longtext";
      }
      if (varcharType.getLengthSafe() <= 255) {
        return "tinytext";
      }
      if (varcharType.getLengthSafe() <= 65535) {
        return "text";
      }
      if (varcharType.getLengthSafe() <= 16777215) {
        return "mediumtext";
      }
      return "longtext";
    }

    return super.toSqlType(type);
  }

  @Override
  public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata) {
    try {
      createTable(tableMetadata, session, tableMetadata.getTable().getTableName());
    } catch (SQLException e) {
      if (SQL_STATE_ER_TABLE_EXISTS_ERROR.equals(e.getSQLState())) {
        throw new PrestoException(ALREADY_EXISTS, e);
      }
      throw new PrestoException(JDBC_ERROR, e);
    }
  }

  @Override
  public void renameColumn(
      ConnectorSession session,
      JdbcIdentity identity,
      JdbcTableHandle handle,
      JdbcColumnHandle jdbcColumn,
      String newColumnName) {
    try (Connection connection = connectionFactory.openConnection(identity)) {
      DatabaseMetaData metadata = connection.getMetaData();
      if (metadata.storesUpperCaseIdentifiers()) {
        newColumnName = newColumnName.toUpperCase(ENGLISH);
      }
      String sql =
          String.format(
              "ALTER TABLE %s RENAME COLUMN %s TO %s",
              quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()),
              quoted(jdbcColumn.getColumnName()),
              quoted(newColumnName));
      execute(connection, sql);
    } catch (SQLException e) {
      // MySQL versions earlier than 8 do not support the above RENAME COLUMN syntax
      if (SQL_STATE_SYNTAX_ERROR.equals(e.getSQLState())) {
        throw new PrestoException(
            NOT_SUPPORTED,
            String.format("Rename column not supported in catalog: '%s'", handle.getCatalogName()),
            e);
      }
      throw new PrestoException(JDBC_ERROR, e);
    }
  }

  @Override
  protected void renameTable(
      JdbcIdentity identity,
      String catalogName,
      SchemaTableName oldTable,
      SchemaTableName newTable) {
    // MySQL doesn't support specifying the catalog name in a rename; by setting the
    // catalogName parameter to null it will be omitted in the alter table statement.
    super.renameTable(identity, null, oldTable, newTable);
  }

  @Override
  public Connection getConnection(ConnectorSession session, JdbcIdentity identity, JdbcSplit split)
      throws SQLException {
    Connection connection = connectionFactory.openConnection(identity);
    TiDBPrestoJdbcSplit tiDBPrestoJdbcSplit = (TiDBPrestoJdbcSplit) split;
    String sessionSnapshot = session.getProperty(TiDBConfig.TIDB_SNAPSHOT_SESSION, String.class);
    long version;
    if (sessionSnapshot != null && !sessionSnapshot.isEmpty()) {
      version = TiDBJdbcUtils.parseVersionFromString(sessionSnapshot);
    } else {
      version = tiDBPrestoJdbcSplit.getVersion();
    }
    try (java.sql.Statement statement = connection.createStatement()) {
      statement.execute(String.format(TiDBJdbcUtils.SET_TS_SQL_FORMAT, version));
    } catch (SQLException e) {
      connection.close();
      throw e;
    }
    return connection;
  }

  @Override
  public ConnectorSplitSource getSplits(
      ConnectorSession session, JdbcIdentity identity, JdbcTableLayoutHandle layoutHandle) {
    try (Connection connection = connectionFactory.openConnection(identity)) {
      long version = TiDBJdbcUtils.queryVersion(connection);
      JdbcTableHandle tableHandle = layoutHandle.getTable();
      SchemaTableName schemaTableName = tableHandle.getSchemaTableName();
      String schemaName = schemaTableName.getSchemaName();
      String tableName = schemaTableName.getTableName();
      List<JdbcSplit> splits =
          TiDBJdbcUtils.querySplits(connection, schemaName, tableName, version, true).stream()
              .map(
                  tiDBJdbcSplit ->
                      new TiDBPrestoJdbcSplit(
                          connectorId,
                          null,
                          tiDBJdbcSplit.getDatabaseName(),
                          tiDBJdbcSplit.getTableName(),
                          layoutHandle.getTupleDomain(),
                          layoutHandle.getAdditionalPredicate(),
                          tiDBJdbcSplit.getStartKey(),
                          tiDBJdbcSplit.getEndKey(),
                          tiDBJdbcSplit.getVersion()))
              .collect(Collectors.toList());
      log.info("Get %s splits for table `%s`.`%s`", splits.size(), schemaName, tableName);
      return new FixedSplitSource(splits);
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public PreparedStatement buildSql(
      ConnectorSession session,
      Connection connection,
      JdbcSplit split,
      List<JdbcColumnHandle> columnHandles)
      throws SQLException {
    TiDBPrestoJdbcSplit tiDBPrestoJdbcSplit = (TiDBPrestoJdbcSplit) split;
    return new TiDBQueryBuilder(identifierQuote)
        .buildSql(
            this,
            session,
            connection,
            split.getCatalogName(),
            split.getSchemaName(),
            split.getTableName(),
            columnHandles,
            split.getTupleDomain(),
            split.getAdditionalPredicate(),
            tiDBPrestoJdbcSplit.getStartKey(),
            tiDBPrestoJdbcSplit.getEndKey(),
            tiDBPrestoJdbcSplit.getVersion());
  }
}
