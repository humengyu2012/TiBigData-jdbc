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

import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.LOOKUP_CACHE_TTL;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.LOOKUP_MAX_RETRIES;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.MAX_RETRY_TIMEOUT;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SINK_MAX_RETRIES;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SINK_PARALLELISM;

import com.google.common.collect.ImmutableSet;
import io.tidb.bigdata.flink.connector.source.SnapshotSourceSemantic;
import io.tidb.bigdata.jdbc.core.TiDBCoreConfig;
import java.util.Arrays;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class TiDBOptions {

  private static ConfigOption<String> required(String key) {
    return ConfigOptions.key(key).stringType().noDefaultValue();
  }

  private static ConfigOption<String> optional(String key, String value) {
    return ConfigOptions.key(key).stringType().defaultValue(value);
  }

  private static ConfigOption<String> optional(String key) {
    return optional(key, null);
  }

  public static final ConfigOption<String> DATABASE_URL = required(TiDBCoreConfig.DATABASE_URL);

  public static final ConfigOption<String> USERNAME = required(TiDBCoreConfig.USERNAME);

  public static final ConfigOption<String> PASSWORD = required(TiDBCoreConfig.PASSWORD);

  public static final ConfigOption<String> DATABASE_NAME = required("tidb.database.name");

  public static final ConfigOption<String> TABLE_NAME = required("tidb.table.name");

  public static final ConfigOption<String> SOURCE_SEMANTIC =
      optional("tidb.source.semantic", SnapshotSourceSemantic.AT_LEAST_ONCE.getSemantic());

  public static final ConfigOption<String> STREAMING_SOURCE = optional("tidb.streaming.source");

  public static final String STREAMING_SOURCE_KAFKA = "kafka";

  public static final Set<String> VALID_STREAMING_SOURCES = ImmutableSet.of(STREAMING_SOURCE_KAFKA);

  public static final ConfigOption<String> STREAMING_CODEC = optional("tidb.streaming.codec");

  public static final String STREAMING_CODEC_JSON = "json";
  public static final String STREAMING_CODEC_CRAFT = "craft";
  public static final String STREAMING_CODEC_CANAL_JSON = "canal-json";
  public static final String STREAMING_CODEC_CANAL_PROTOBUF = "canal-protobuf";
  public static final Set<String> VALID_STREAMING_CODECS =
      ImmutableSet.of(
          STREAMING_CODEC_CRAFT,
          STREAMING_CODEC_JSON,
          STREAMING_CODEC_CANAL_JSON,
          STREAMING_CODEC_CANAL_PROTOBUF);

  // Options for catalog
  public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS =
      ConfigOptions.key("tidb.streaming.ignore-parse-errors")
          .booleanType()
          .defaultValue(false)
          .withDescription(
              "Optional flag to skip change events with parse errors instead of failing;\n"
                  + "fields are set to null in case of errors, false by default.");

  // For example:
  // 'tidb.metadata.included' = 'commit_timestamp=_commit_timestamp,commit_version=_commit_version'
  public static final String METADATA_INCLUDED = "tidb.metadata.included";
  public static final String METADATA_INCLUDED_ALL = "*";

  // During the streaming phase, set a timestamp to avoid consuming expired data,
  // with the default being one minute subtracted from the snapshot time.
  // Additionally, we can set this value to less than or equal to 0 to disable this feature.
  public static final String STARTING_OFFSETS_TS = "tidb.streaming.source.starting.offsets.ts";
  public static final long MINUTE = 60 * 1000L;

  /**
   * see {@link org.apache.flink.connector.jdbc.table.JdbcConnectorOptions}
   *
   * @return
   */
  private static Set<ConfigOption<?>> jdbcOptionalOptions() {
    return ImmutableSet.of(
        SINK_PARALLELISM,
        MAX_RETRY_TIMEOUT,
        LOOKUP_CACHE_MAX_ROWS,
        LOOKUP_CACHE_TTL,
        LOOKUP_MAX_RETRIES,
        SINK_BUFFER_FLUSH_MAX_ROWS,
        SINK_BUFFER_FLUSH_INTERVAL,
        SINK_MAX_RETRIES);
  }

  public static Set<ConfigOption<?>> requiredOptions() {
    return ImmutableSet.of(DATABASE_URL, USERNAME);
  }

  public static Set<ConfigOption<?>> optionalOptions() {
    return ImmutableSet.<ConfigOption<?>>builder()
        .addAll(jdbcOptionalOptions())
        .add(PASSWORD, STREAMING_SOURCE, SOURCE_SEMANTIC)
        .build();
  }

  public enum SinkImpl {
    JDBC,
    TIKV;

    public static SinkImpl fromString(String s) {
      for (SinkImpl value : values()) {
        if (value.name().equalsIgnoreCase(s)) {
          return value;
        }
      }
      throw new IllegalArgumentException(
          "Property sink.impl must be one of: " + Arrays.toString(values()));
    }
  }

  public enum SinkTransaction {
    GLOBAL,
    MINIBATCH,
    CHECKPOINT;

    public static SinkTransaction fromString(String s) {
      for (SinkTransaction value : values()) {
        if (value.name().equalsIgnoreCase(s)) {
          return value;
        }
      }
      throw new IllegalArgumentException(
          "Property sink.transaction must be one of: " + Arrays.toString(values()));
    }
  }
}
