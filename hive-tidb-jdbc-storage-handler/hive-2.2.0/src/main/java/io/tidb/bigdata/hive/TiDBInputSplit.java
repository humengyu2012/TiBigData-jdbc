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

import io.tidb.bigdata.jdbc.core.TiDBJdbcSplit;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

public class TiDBInputSplit extends FileSplit {

  private Path path;
  private List<TiDBJdbcSplit> jdbcSplits;

  public TiDBInputSplit() {}

  public TiDBInputSplit(Path path, List<TiDBJdbcSplit> jdbcSplits) {
    this.path = path;
    this.jdbcSplits = jdbcSplits;
  }

  public List<TiDBJdbcSplit> getJdbcSplits() {
    return jdbcSplits;
  }

  @Override
  public Path getPath() {
    return path;
  }

  @Override
  public long getLength() {
    return 0;
  }

  @Override
  public String[] getLocations() throws IOException {
    return new String[0];
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(path.toString());
    dataOutput.writeInt(jdbcSplits.size());
    for (int i = 0; i < jdbcSplits.size(); i++) {
      TiDBJdbcSplit split = jdbcSplits.get(i);
      dataOutput.writeUTF(split.getDatabaseName());
      dataOutput.writeUTF(split.getTableName());
      dataOutput.writeUTF(split.getStartKey());
      dataOutput.writeUTF(split.getEndKey());
      dataOutput.writeLong(split.getVersion());
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.path = new Path(dataInput.readUTF());
    int size = dataInput.readInt();
    List<TiDBJdbcSplit> splits = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      String databaseName = dataInput.readUTF();
      String tableName = dataInput.readUTF();
      String startKey = dataInput.readUTF();
      String endKey = dataInput.readUTF();
      long version = dataInput.readLong();
      TiDBJdbcSplit split = new TiDBJdbcSplit(databaseName, tableName, startKey, endKey, version);
      splits.add(split);
    }
    this.jdbcSplits = splits;
  }
}
