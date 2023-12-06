/*
 * Copyright 2022 TiDB Project Authors.
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

package io.tidb.bigdata.hive;

import static io.tidb.bigdata.hive.TiDBConstant.DATABASE_NAME;
import static io.tidb.bigdata.hive.TiDBConstant.TABLE_NAME;

import io.tidb.bigdata.jdbc.core.TiDBColumn;
import io.tidb.bigdata.jdbc.core.TiDBCoreConfig;
import io.tidb.bigdata.jdbc.core.TiDBJdbcUtils;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TiDBSerde extends AbstractSerDe {

  private String databaseName;
  private String tableName;
  private Properties properties;
  private List<TiDBColumn> columns;

  @Override
  public void initialize(@Nullable Configuration configuration, Properties properties)
      throws SerDeException {
    this.tableName =
        Objects.requireNonNull(properties.getProperty(TABLE_NAME), TABLE_NAME + " can not be null");
    this.databaseName =
        Objects.requireNonNull(
            properties.getProperty(DATABASE_NAME), DATABASE_NAME + " can not be null");
    this.properties = properties;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return MapWritable.class;
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
    Object[] objects = (Object[]) o;
    MapWritable mapWritable = new MapWritable();
    List<TiDBColumn> columns = getColumns();
    for (int i = 0; i < columns.size(); i++) {
      TiDBColumn column = columns.get(i);
      String name = column.getName();
      Object value = objects[i];
      mapWritable.put(new Text(name), TypeUtils.toWriteable(value, column));
    }
    return mapWritable;
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    MapWritable mapWritable = (MapWritable) writable;
    List<TiDBColumn> columns = getColumns();
    Object[] objects = new Object[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      TiDBColumn column = columns.get(i);
      Writable value = mapWritable.get(new Text(column.getName()));
      if (value instanceof NullWritable) {
        value = null;
      }
      objects[i] = value;
    }
    return objects;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    List<TiDBColumn> columns = getColumns();
    List<ObjectInspector> list = new ArrayList<>();
    columns.forEach(column -> list.add(TypeUtils.toObjectInspector(column)));
    List<String> columnNames =
        columns.stream().map(TiDBColumn::getName).collect(Collectors.toList());
    return ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, list);
  }

  private synchronized List<TiDBColumn> getColumns() {
    if (columns == null) {
      TiDBCoreConfig config = TiDBCoreConfig.fromProperties((Map) properties);
      try (Connection connection = TiDBJdbcUtils.establishNewConnection(config)) {
        this.columns = TiDBJdbcUtils.queryTiDBColumns(connection, databaseName, tableName);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
    return columns;
  }
}
