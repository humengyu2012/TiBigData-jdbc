package io.tidb.bigdata.prestodb;

import com.facebook.presto.plugin.jdbc.JdbcSessionPropertiesProvider;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import java.util.List;

public class TiDBSessionPropertiesProvider implements JdbcSessionPropertiesProvider {

  @Override
  public List<PropertyMetadata<?>> getSessionProperties() {
    return ImmutableList.of(
        PropertyMetadata.stringProperty(
            TiDBConfig.TIDB_SNAPSHOT_SESSION,
            "tidb snapshot, it could be tso(like '446101758670536705') "
                + "or datetime(like '2016-10-08 16:45:26')",
            null,
            false));
  }
}
