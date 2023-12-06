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

import io.tidb.bigdata.jdbc.core.TiDBColumn.JavaType;
import io.tidb.bigdata.jdbc.core.TiDBColumn.TiDBType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TypeUtils {

  public static PrimitiveObjectInspector toObjectInspector(TiDBType type) {
    JavaType javaType = type.getJavaType();
    int precision = type.getPrecision();
    int scale = type.getScale();
    PrimitiveCategory primitiveCategory;
    switch (javaType) {
      case BOOLEAN:
        primitiveCategory = PrimitiveCategory.BOOLEAN;
        break;
      case BYTE:
      case SHORT:
      case INTEGER:
        primitiveCategory = PrimitiveCategory.INT;
        break;
      case LONG:
        primitiveCategory = PrimitiveCategory.LONG;
        break;
      case BIGINTEGER:
      case BIGDECIMAL:
        primitiveCategory = PrimitiveCategory.DECIMAL;
        break;
      case FLOAT:
        primitiveCategory = PrimitiveCategory.FLOAT;
        break;
      case DOUBLE:
        primitiveCategory = PrimitiveCategory.DOUBLE;
        break;
      case LOCALDATATIME:
      case TIMESTAMP:
        primitiveCategory = PrimitiveCategory.TIMESTAMP;
        break;
      case DATE:
        primitiveCategory = PrimitiveCategory.DATE;
        break;
      case BYTES:
        primitiveCategory = PrimitiveCategory.BINARY;
        break;
      case TIME:
      case STRING:
        primitiveCategory = PrimitiveCategory.STRING;
        break;
      default:
        throw new IllegalArgumentException(
            String.format("Can not get hive type by tidb type: %s", type));
    }
    if (primitiveCategory == PrimitiveCategory.DECIMAL) {
      return new WritableHiveDecimalObjectInspector(new DecimalTypeInfo(precision, scale));
    } else {
      return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(primitiveCategory);
    }
  }

  public static Writable toWriteable(Object object, TiDBType type) {
    if (object == null) {
      return NullWritable.get();
    }
    switch (type.getJavaType()) {
      case BOOLEAN:
        return new BooleanWritable((boolean) object);
      case BYTE:
      case SHORT:
      case INTEGER:
        return new IntWritable(Integer.parseInt(object.toString()));
      case LONG:
        return new LongWritable((long) object);
      case BIGINTEGER:
        return new HiveDecimalWritable(HiveDecimal.create((BigInteger) object));
      case BIGDECIMAL:
        return new HiveDecimalWritable(HiveDecimal.create((BigDecimal) object));
      case FLOAT:
        return new FloatWritable((float) object);
      case DOUBLE:
        return new DoubleWritable((double) object);
      case LOCALDATATIME:
        return new TimestampWritable(Timestamp.valueOf((LocalDateTime) object));
      case TIMESTAMP:
        return new TimestampWritable((Timestamp) object);
      case DATE:
        return new DateWritable((Date) object);
      case BYTES:
        return new BytesWritable((byte[]) object);
      case TIME:
      case STRING:
        return new Text(object.toString());
      default:
        throw new IllegalArgumentException(
            String.format(
                "Can not covert tidb java type to writable type, object = %s, type = %s",
                object, type));
    }
  }
}
