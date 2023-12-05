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

import static com.google.common.base.MoreObjects.firstNonNull;

import com.facebook.presto.plugin.jdbc.JdbcConnectorFactory;
import com.facebook.presto.plugin.jdbc.JdbcHandleResolver;
import com.facebook.presto.plugin.jdbc.JdbcPlugin;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import java.lang.reflect.Field;

public class TiDBPlugin extends JdbcPlugin {

  private static final String TIDB = "tidb";

  public TiDBPlugin() {
    super(TIDB, new TiDBClientModule());
  }

  @Override
  public Iterable<ConnectorFactory> getConnectorFactories() {
    Module module;
    try {
      Field field = JdbcPlugin.class.getDeclaredField("module");
      field.setAccessible(true);
      module = (Module) field.get(this);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    return ImmutableList.of(new TiDBJdbcConnectorFactory(TIDB, module, getClassLoader()));
  }

  private static ClassLoader getClassLoader() {
    return firstNonNull(
        Thread.currentThread().getContextClassLoader(), TiDBPlugin.class.getClassLoader());
  }

  public static class TiDBJdbcConnectorFactory extends JdbcConnectorFactory {

    public TiDBJdbcConnectorFactory(String name, Module module, ClassLoader classLoader) {
      super(name, module, classLoader);
    }

    @Override
    public ConnectorHandleResolver getHandleResolver() {
      return new TiDBJdbcHandleResolver();
    }
  }

  public static class TiDBJdbcHandleResolver extends JdbcHandleResolver {

    @Override
    public Class<? extends ConnectorSplit> getSplitClass() {
      return TiDBPrestoJdbcSplit.class;
    }
  }
}
