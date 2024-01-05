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

package io.tidb.bigdata.flink.connector.source.reader;

import com.google.common.collect.ImmutableList;
import io.tidb.bigdata.flink.connector.source.SnapshotSourceSemantic;
import io.tidb.bigdata.flink.connector.source.TiDBSchemaAdapter;
import io.tidb.bigdata.flink.connector.source.split.TiDBSourceSplit;
import io.tidb.bigdata.jdbc.core.TiDBColumn;
import io.tidb.bigdata.jdbc.core.TiDBCoreConfig;
import io.tidb.bigdata.jdbc.core.TiDBJdbcClient;
import io.tidb.bigdata.jdbc.core.TiDBJdbcRecordCursor;
import io.tidb.bigdata.jdbc.core.TiDBJdbcSplit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.data.RowData;

public class TiDBSourceReader implements SourceReader<RowData, TiDBSourceSplit> {

  private final Queue<TiDBSourceSplit> remainingSplits;
  private final SourceReaderContext context;
  private final Map<String, String> properties;
  private final List<TiDBColumn> columns;
  private final TiDBSchemaAdapter schema;
  private final String whereCondition;
  private final Integer limit;
  private final SnapshotSourceSemantic semantic;

  private TiDBJdbcClient jdbcClient;

  /** The availability future. This reader is available as soon as a split is assigned. */
  private CompletableFuture<Void> availability;

  private TiDBSourceSplit currentSplit;
  private TiDBJdbcRecordCursor cursor;

  private boolean noMoreSplits;

  public TiDBSourceReader(
      SourceReaderContext context,
      Map<String, String> properties,
      List<TiDBColumn> columns,
      TiDBSchemaAdapter schema,
      String whereCondition,
      Integer limit,
      SnapshotSourceSemantic semantic) {
    this.context = context;
    this.properties = properties;
    this.columns = columns;
    this.schema = schema;
    this.whereCondition = whereCondition;
    this.limit = limit;
    this.availability = new CompletableFuture<>();
    this.remainingSplits = new ArrayDeque<>();
    this.semantic = semantic;
  }

  @Override
  public void start() {
    // request a split if we don't have one
    if (remainingSplits.isEmpty()) {
      context.sendSplitRequest();
    }
    jdbcClient = TiDBJdbcClient.create(TiDBCoreConfig.fromProperties(properties));
  }

  private void finishSplit() {
    currentSplit = null;
    if (cursor != null) {
      cursor.close();
      cursor = null;
    }
    // request another split if no other is left
    // we do this only here in the finishSplit part to avoid requesting a split
    // whenever the reader is polled and doesn't currently have a split
    if (remainingSplits.isEmpty() && !noMoreSplits) {
      context.sendSplitRequest();
    }
  }

  private InputStatus tryMoveToNextSplit() {
    currentSplit = remainingSplits.poll();
    if (currentSplit != null) {
      TiDBJdbcSplit tiDBJdbcSplit = currentSplit.getSplit();
      cursor = jdbcClient.scan(ImmutableList.of(tiDBJdbcSplit), columns, limit, whereCondition);
      return InputStatus.MORE_AVAILABLE;
    } else if (noMoreSplits) {
      return InputStatus.END_OF_INPUT;
    } else {
      // ensure we are not called in a loop by resetting the availability future
      if (availability.isDone()) {
        availability = new CompletableFuture<>();
      }
      return InputStatus.NOTHING_AVAILABLE;
    }
  }

  @Override
  public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
    if (cursor != null && cursor.advanceNextPosition()) {
      output.collect(schema.convert(currentSplit.getSplit().getVersion(), cursor));
      return InputStatus.MORE_AVAILABLE;
    } else {
      finishSplit();
    }
    return tryMoveToNextSplit();
  }

  private TiDBSourceSplit createNewSplit() {
    // TODO: Only at least once is supported now, exactly once will be supported later
    return new TiDBSourceSplit(currentSplit.getSplit());
  }

  @Override
  public List<TiDBSourceSplit> snapshotState(long checkpointId) {
    if (currentSplit == null && remainingSplits.isEmpty()) {
      return Collections.emptyList();
    }
    final ArrayList<TiDBSourceSplit> splits = new ArrayList<>(1 + remainingSplits.size());
    if (currentSplit != null) {
      // Add back to snapshot
      if (semantic == SnapshotSourceSemantic.AT_LEAST_ONCE
          || cursor == null
          || cursor.currentResultSet() == null) {
        splits.add(currentSplit);
      } else {
        splits.add(createNewSplit());
      }
    }
    splits.addAll(remainingSplits);
    return splits;
  }

  @Override
  public CompletableFuture<Void> isAvailable() {
    return availability;
  }

  @Override
  public void addSplits(List<TiDBSourceSplit> splits) {
    remainingSplits.addAll(splits);
    // set availability so that pollNext is actually called
    availability.complete(null);
  }

  @Override
  public void notifyNoMoreSplits() {
    this.noMoreSplits = true;
    // set availability so that pollNext is actually called
    availability.complete(null);
  }

  @Override
  public void close() throws Exception {
    if (cursor != null) {
      cursor.close();
    }
    if (jdbcClient != null) {
      jdbcClient.close();
    }
  }
}
