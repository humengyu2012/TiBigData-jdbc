/*
 * Copyright 2017 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.tidb.bigdata.tidb.meta;

import org.tikv.common.exception.TiClientInternalException;

public enum SchemaState {
  StateNone(0),
  StateDeleteOnly(1),
  StateWriteOnly(2),
  StateWriteReorganization(3),
  StateDeleteReorganization(4),
  StatePublic(5);

  private final int state;

  SchemaState(int state) {
    this.state = state;
  }

  public static SchemaState fromValue(int b) {
    for (SchemaState e : SchemaState.values()) {
      if (e.state == b) {
        return e;
      }
    }
    throw new TiClientInternalException("Invalid SchemaState code: " + b);
  }

  public int getStateCode() {
    return state;
  }
}