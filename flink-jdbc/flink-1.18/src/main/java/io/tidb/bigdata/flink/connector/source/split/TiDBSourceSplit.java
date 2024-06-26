/*
 * Copyright 2021 TiDB Project Authors.
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

package io.tidb.bigdata.flink.connector.source.split;

import io.tidb.bigdata.jdbc.core.TiDBJdbcSplit;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;
import org.apache.flink.api.connector.source.SourceSplit;

public class TiDBSourceSplit implements Serializable, SourceSplit {

  private final TiDBJdbcSplit split;

  public TiDBSourceSplit(TiDBJdbcSplit split) {
    this.split = split;
  }

  @Override
  public String splitId() {
    return split.toString();
  }

  public TiDBJdbcSplit getSplit() {
    return split;
  }

  @Override
  public int hashCode() {
    return split.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TiDBSourceSplit)) {
      return false;
    }
    return Objects.equals(split, ((TiDBSourceSplit) o).split);
  }

  public void serialize(DataOutputStream dos) throws IOException {
    dos.writeUTF(split.getDatabaseName());
    dos.writeUTF(split.getTableName());
    dos.writeUTF(split.getStartKey());
    dos.writeUTF(split.getEndKey());
    dos.writeLong(split.getVersion());
  }

  public static TiDBSourceSplit deserialize(DataInputStream dis) throws IOException {
    String databaseName = dis.readUTF();
    String tableName = dis.readUTF();
    String startKey = dis.readUTF();
    String endKey = dis.readUTF();
    long version = dis.readLong();
    return new TiDBSourceSplit(
        new TiDBJdbcSplit(databaseName, tableName, startKey, endKey, version));
  }
}
