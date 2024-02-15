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

import com.google.common.collect.ImmutableSet;
import io.tidb.bigdata.jdbc.core.TiDBCoreConfig;
import java.util.Set;

public class TiDBConstant {

  public static final String EMPTY_STRING = "";
  public static final String TABLE_NAME = "tidb.table.name";
  public static final String DATABASE_NAME = "tidb.database.name";
  public static final String REGIONS_PER_SPLIT = "tidb.regions-per-split";

  // configurations that do not take effect within session
  public static final Set<String> IMMUTABLE_CONFIG =
      ImmutableSet.of(
          TiDBCoreConfig.DATABASE_URL, TiDBCoreConfig.USERNAME, TiDBCoreConfig.PASSWORD);
}