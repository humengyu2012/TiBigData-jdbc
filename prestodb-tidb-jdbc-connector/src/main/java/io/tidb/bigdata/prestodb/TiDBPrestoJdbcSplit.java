package io.tidb.bigdata.prestodb;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.plugin.jdbc.optimization.JdbcExpression;
import com.facebook.presto.spi.ColumnHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Optional;
import javax.annotation.Nullable;

public class TiDBPrestoJdbcSplit extends JdbcSplit {

  private final String startKey;
  private final String endKey;
  private final long version;

  @JsonCreator
  public TiDBPrestoJdbcSplit(
      @JsonProperty("connectorId") String connectorId,
      @JsonProperty("catalogName") @Nullable String catalogName,
      @JsonProperty("schemaName") @Nullable String schemaName,
      @JsonProperty("tableName") String tableName,
      @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain,
      @JsonProperty("additionalProperty") Optional<JdbcExpression> additionalPredicate,
      @JsonProperty("startKey") String startKey,
      @JsonProperty("endKey") String endKey,
      @JsonProperty("version") long version) {
    super(connectorId, catalogName, schemaName, tableName, tupleDomain, additionalPredicate);
    this.startKey = startKey;
    this.endKey = endKey;
    this.version = version;
  }

  @JsonProperty
  public String getStartKey() {
    return startKey;
  }

  @JsonProperty
  public String getEndKey() {
    return endKey;
  }

  @JsonProperty
  public long getVersion() {
    return version;
  }

  @Override
  public Object getInfo() {
    return this;
  }
}
