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

package io.tidb.bigdata.flink.connector.source;

import static io.tidb.bigdata.flink.connector.TiDBOptions.SOURCE_SEMANTIC;

import io.tidb.bigdata.flink.connector.source.enumerator.TiDBSourceSplitEnumState;
import io.tidb.bigdata.flink.connector.source.enumerator.TiDBSourceSplitEnumStateSerializer;
import io.tidb.bigdata.flink.connector.source.enumerator.TiDBSourceSplitEnumerator;
import io.tidb.bigdata.flink.connector.source.reader.TiDBSourceReader;
import io.tidb.bigdata.flink.connector.source.split.TiDBSourceSplit;
import io.tidb.bigdata.flink.connector.source.split.TiDBSourceSplitSerializer;
import io.tidb.bigdata.jdbc.core.TiDBColumn;
import io.tidb.bigdata.jdbc.core.TiDBCoreConfig;
import io.tidb.bigdata.jdbc.core.TiDBJdbcClient;
import io.tidb.bigdata.jdbc.core.TiDBJdbcUtils;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;

public class SnapshotSource
    implements Source<RowData, TiDBSourceSplit, TiDBSourceSplitEnumState>,
        ResultTypeQueryable<RowData> {

  private final String databaseName;
  private final String tableName;
  private final Map<String, String> properties;
  private final TiDBSchemaAdapter schema;
  private final String whereCondition;
  private final Integer limit;
  private final List<TiDBColumn> columns;
  private final long version;
  private final List<TiDBSourceSplit> splits;
  private final SnapshotSourceSemantic semantic;

  public SnapshotSource(
      String databaseName,
      String tableName,
      Map<String, String> properties,
      TiDBSchemaAdapter schema,
      String whereCondition,
      Integer limit) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.properties = properties;
    this.schema = schema;
    this.limit = limit;
    this.whereCondition = whereCondition;
    try (TiDBJdbcClient jdbcClient =
        TiDBJdbcClient.create(TiDBCoreConfig.fromProperties(properties))) {
      this.columns =
          jdbcClient.getColumnsByNames(
              databaseName, tableName, Arrays.asList(schema.getPhysicalFieldNamesWithoutMeta()));
      this.version = getOptionalVersion().orElseGet(jdbcClient::getApproximateSnapshotVersion);
      this.splits =
          jdbcClient.getSplits(databaseName, tableName, version, true).stream()
              .map(TiDBSourceSplit::new)
              .collect(Collectors.toList());
      this.semantic =
          SnapshotSourceSemantic.fromString(
              properties.getOrDefault(SOURCE_SEMANTIC.key(), SOURCE_SEMANTIC.defaultValue()));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Boundedness getBoundedness() {
    return Boundedness.BOUNDED;
  }

  @Override
  public SourceReader<RowData, TiDBSourceSplit> createReader(SourceReaderContext context)
      throws Exception {
    return new TiDBSourceReader(
        context, properties, columns, schema, whereCondition, limit, semantic);
  }

  @Override
  public SplitEnumerator<TiDBSourceSplit, TiDBSourceSplitEnumState> createEnumerator(
      SplitEnumeratorContext<TiDBSourceSplit> context) throws Exception {
    return new TiDBSourceSplitEnumerator(splits, version, context);
  }

  @Override
  public SplitEnumerator<TiDBSourceSplit, TiDBSourceSplitEnumState> restoreEnumerator(
      SplitEnumeratorContext<TiDBSourceSplit> context, TiDBSourceSplitEnumState state) {
    return new TiDBSourceSplitEnumerator(state.getSplits(), version, context);
  }

  @Override
  public SimpleVersionedSerializer<TiDBSourceSplit> getSplitSerializer() {
    return new TiDBSourceSplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<TiDBSourceSplitEnumState> getEnumeratorCheckpointSerializer() {
    return new TiDBSourceSplitEnumStateSerializer();
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    return schema.getProducedType();
  }

  private Optional<Long> getOptionalVersion() {
    return Optional.ofNullable(properties.get(TiDBCoreConfig.TIDB_SNAPSHOT))
        .filter(StringUtils::isNotEmpty)
        .map(TiDBJdbcUtils::parseVersionFromString);
  }
}
