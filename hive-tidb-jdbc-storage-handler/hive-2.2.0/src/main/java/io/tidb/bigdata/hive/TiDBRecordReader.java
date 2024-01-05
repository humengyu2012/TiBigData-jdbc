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

import io.tidb.bigdata.jdbc.core.TiDBColumn;
import io.tidb.bigdata.jdbc.core.TiDBCoreConfig;
import io.tidb.bigdata.jdbc.core.TiDBJdbcRecordCursor;
import io.tidb.bigdata.jdbc.core.TiDBJdbcSplit;
import io.tidb.bigdata.jdbc.core.TiDBJdbcUtils;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TiDBRecordReader implements RecordReader<LongWritable, MapWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(TiDBRecordReader.class);

  private final TiDBInputSplit tidbInputSplit;
  private final Map<String, String> properties;
  private final List<TiDBJdbcSplit> jdbcSplits;

  private long pos;
  private Connection connection;
  private TiDBJdbcRecordCursor cursor;
  private List<TiDBColumn> columns;

  public TiDBRecordReader(InputSplit split, Map<String, String> properties) {
    this.tidbInputSplit = (TiDBInputSplit) split;
    this.properties = properties;
    this.jdbcSplits = tidbInputSplit.getJdbcSplits();
  }

  private void establishConnection() throws SQLException {
    LOG.info("Establish client session");
    this.connection =
        TiDBJdbcUtils.establishNewConnection(TiDBCoreConfig.fromProperties(properties));
  }

  private void initCursor() throws SQLException {
    TiDBJdbcSplit split = jdbcSplits.get(0);
    columns =
        TiDBJdbcUtils.queryTiDBColumns(connection, split.getDatabaseName(), split.getTableName());
    cursor = TiDBJdbcUtils.scan(connection, jdbcSplits, columns, null, null);
  }

  @Override
  public boolean next(LongWritable longWritable, MapWritable mapWritable) throws IOException {
    try {
      if (jdbcSplits.isEmpty()) {
        return false;
      }
      if (connection == null) {
        establishConnection();
      }
      if (cursor == null) {
        initCursor();
      }

      if (!cursor.advanceNextPosition()) {
        return false;
      }
      pos++;
      for (int i = 0; i < cursor.fieldCount(); i++) {
        TiDBColumn column = columns.get(i);
        String name = column.getName();
        mapWritable.put(
            new Text(name), TypeUtils.toWriteable(cursor.getObject(i), column.getType()));
      }
      return true;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public LongWritable createKey() {
    return new LongWritable();
  }

  @Override
  public MapWritable createValue() {
    return new MapWritable();
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public void close() throws IOException {
    try {
      if (cursor != null) {
        cursor.close();
      }
    } catch (Exception e) {
      LOG.warn("Can not close cursor", e);
    }
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (Exception e) {
      LOG.warn("Can not close connection", e);
    }
  }

  @Override
  public float getProgress() throws IOException {
    return 0;
  }
}
