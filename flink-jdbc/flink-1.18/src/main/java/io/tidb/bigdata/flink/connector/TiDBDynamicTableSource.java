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

package io.tidb.bigdata.flink.connector;

import io.tidb.bigdata.flink.connector.source.TiDBMetadata;
import io.tidb.bigdata.flink.connector.source.TiDBSchemaAdapter;
import io.tidb.bigdata.flink.connector.source.TiDBSourceBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.flink.connector.jdbc.databases.mysql.dialect.MySqlDialect;
import org.apache.flink.connector.jdbc.table.JdbcFilterPushdownPreparedStatementVisitor;
import org.apache.flink.connector.jdbc.table.ParameterizedPredicate;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TiDBDynamicTableSource
    implements ScanTableSource,
        LookupTableSource,
        SupportsProjectionPushDown,
        SupportsFilterPushDown,
        SupportsLimitPushDown,
        SupportsReadingMetadata {

  private static final Logger LOG = LoggerFactory.getLogger(TiDBDynamicTableSource.class);

  private final ResolvedCatalogTable table;
  private final ChangelogMode changelogMode;
  private TiDBSchemaAdapter schema;
  private Integer limit;
  private String whereCondition;

  public TiDBDynamicTableSource(ResolvedCatalogTable table, ChangelogMode changelogMode) {
    this.table = table;
    this.changelogMode = changelogMode;
    this.schema = new TiDBSchemaAdapter(table);
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return changelogMode;
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
    return SourceProvider.of(new TiDBSourceBuilder(table, schema, whereCondition, limit).build());
  }

  @Override
  public DynamicTableSource copy() {
    TiDBDynamicTableSource otherSource = new TiDBDynamicTableSource(table, changelogMode);
    otherSource.schema = this.schema;
    return otherSource;
  }

  @Override
  public String asSummaryString() {
    return "";
  }

  @Override
  public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean supportsNestedProjection() {
    return false;
  }

  @Override
  public void applyProjection(int[][] projectedFields) {
    schema.applyProjectedFields(Arrays.stream(projectedFields).mapToInt(f -> f[0]).toArray());
  }

  private Optional<ParameterizedPredicate> parseFilterToPredicate(ResolvedExpression filter) {
    MySqlDialect mySqlDialect = new MySqlDialect();
    if (filter instanceof CallExpression) {
      CallExpression callExp = (CallExpression) filter;
      return callExp.accept(
          new JdbcFilterPushdownPreparedStatementVisitor(mySqlDialect::quoteIdentifier));
    }
    return Optional.empty();
  }

  @Override
  public Result applyFilters(List<ResolvedExpression> filters) {
    List<ResolvedExpression> acceptedFilters = new ArrayList<>();
    List<ResolvedExpression> remainingFilters = new ArrayList<>();
    List<String> resolvedPredicates = new ArrayList<>();

    for (ResolvedExpression filter : filters) {
      Optional<ParameterizedPredicate> simplePredicate = parseFilterToPredicate(filter);
      if (simplePredicate.isPresent()) {
        acceptedFilters.add(filter);
        ParameterizedPredicate pred = simplePredicate.get();
        resolvedPredicates.add(pred.getPredicate());
      } else {
        remainingFilters.add(filter);
      }
    }

    if (!resolvedPredicates.isEmpty()) {
      this.whereCondition =
          resolvedPredicates.stream()
              .map(pred -> String.format("(%s)", pred))
              .collect(Collectors.joining(" AND "));
      LOG.info("Apply: \n{}\n to where condition", whereCondition);
    }

    return Result.of(acceptedFilters, remainingFilters);
  }

  private String getRequiredProperties(String key) {
    return Preconditions.checkNotNull(table.getOptions().get(key), key + " can not be null");
  }

  @Override
  public void applyLimit(long limit) {
    this.limit = limit > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) limit;
  }

  @Override
  public Map<String, DataType> listReadableMetadata() {
    return Arrays.stream(TiDBMetadata.values())
        .collect(Collectors.toMap(TiDBMetadata::getKey, TiDBMetadata::getType));
  }

  @Override
  public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
    schema.applyReadableMetadata(metadataKeys, producedDataType);
  }
}
