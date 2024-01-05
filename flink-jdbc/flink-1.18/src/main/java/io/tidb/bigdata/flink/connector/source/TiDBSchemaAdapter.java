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

import static io.tidb.bigdata.flink.connector.TiDBOptions.METADATA_INCLUDED;
import static io.tidb.bigdata.flink.connector.TiDBOptions.METADATA_INCLUDED_ALL;

import com.google.common.collect.ImmutableMap;
import io.tidb.bigdata.flink.format.cdc.CDCMetadata;
import io.tidb.bigdata.flink.format.cdc.CDCSchemaAdapter;
import io.tidb.bigdata.jdbc.core.TiDBColumn.TiDBType;
import io.tidb.bigdata.jdbc.core.TiDBJdbcRecordCursor;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.RowKind;

public class TiDBSchemaAdapter implements Serializable {

  private final Map<String, String> properties;

  private DataType rowDataType;
  private List<String> fieldNames;
  private List<DataType> fieldTypes;
  private LinkedHashMap<String, TiDBMetadata> metadata;

  public TiDBSchemaAdapter(ResolvedCatalogTable table) {
    this.properties = table.getOptions();
    this.metadata = new LinkedHashMap<>();
    ResolvedSchema schema = table.getResolvedSchema();
    Field[] fields =
        schema.getColumns().stream()
            .filter(Column::isPhysical)
            .map(
                c ->
                    DataTypes.FIELD(
                        c.getName(), DataTypeUtils.removeTimeAttribute(c.getDataType())))
            .toArray(Field[]::new);
    buildFields(fields);
  }

  private void buildFields(Field[] fields) {
    this.rowDataType = DataTypes.ROW(fields).notNull();
    this.fieldNames = Arrays.stream(fields).map(Field::getName).collect(Collectors.toList());
    this.fieldTypes = Arrays.stream(fields).map(Field::getDataType).collect(Collectors.toList());
  }

  public void applyProjectedFields(int[] projectedFields) {
    if (projectedFields == null) {
      return;
    }
    LinkedHashMap<String, TiDBMetadata> metadata = new LinkedHashMap<>();
    Field[] fields = new Field[projectedFields.length];
    for (int i = 0; i <= projectedFields.length - 1; i++) {
      String fieldName = fieldNames.get(projectedFields[i]);
      DataType fieldType = fieldTypes.get(projectedFields[i]);
      fields[i] = DataTypes.FIELD(fieldName, fieldType);
      if (this.metadata.containsKey(fieldName)) {
        metadata.put(fieldName, this.metadata.get(fieldName));
      }
    }
    this.metadata = metadata;
    buildFields(fields);
  }

  public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
    rowDataType = producedDataType;
    // append meta columns
    this.metadata = new LinkedHashMap<>();
    for (String key : metadataKeys) {
      TiDBMetadata tiDBMetadata = TiDBMetadata.fromKey(key);
      fieldNames.add(key);
      fieldTypes.add(tiDBMetadata.getType());
      this.metadata.put(key, tiDBMetadata);
    }
  }

  private Object[] makeRow(long version) {
    Object[] objects = new Object[fieldNames.size()];
    for (int i = fieldNames.size() - metadata.size(); i <= fieldNames.size() - 1; i++) {
      objects[i] = metadata.get(fieldNames.get(i)).extract(version);
    }
    return objects;
  }

  public String[] getPhysicalFieldNamesWithoutMeta() {
    return fieldNames.stream().filter(name -> !metadata.containsKey(name)).toArray(String[]::new);
  }

  public GenericRowData convert(long version, TiDBJdbcRecordCursor cursor) {
    Object[] objects = makeRow(version);
    for (int idx = 0; idx < fieldNames.size() - metadata.size(); idx++) {
      Object object;
      try {
        object = cursor.getObject(idx);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
      objects[idx] = toRowDataType(getObjectWithDataType(object, cursor.getType(idx)));
    }
    return GenericRowData.ofKind(RowKind.INSERT, objects);
  }

  // These two methods were copied from flink-base as some interfaces changed in 1.13 made
  // it very hard to reuse code in flink-base
  private static Object stringToFlink(Object object) {
    return StringData.fromString(object.toString());
  }

  private static Object bigDecimalToFlink(Object object) {
    BigDecimal bigDecimal = (BigDecimal) object;
    return DecimalData.fromBigDecimal(bigDecimal, bigDecimal.precision(), bigDecimal.scale());
  }

  private static Object localDateToFlink(Object object) {
    LocalDate localDate = (LocalDate) object;
    return (int) localDate.toEpochDay();
  }

  private static Object localDateTimeToFlink(Object object) {
    return TimestampData.fromLocalDateTime((LocalDateTime) object);
  }

  private static Object localTimeToFlink(Object object) {
    LocalTime localTime = (LocalTime) object;
    return (int) (localTime.toNanoOfDay() / (1000 * 1000));
  }

  public static Map<Class<?>, Function<Object, Object>> ROW_DATA_CONVERTERS =
      ImmutableMap.of(
          String.class,
          TiDBSchemaAdapter::stringToFlink,
          BigDecimal.class,
          TiDBSchemaAdapter::bigDecimalToFlink,
          LocalDate.class,
          TiDBSchemaAdapter::localDateToFlink,
          LocalDateTime.class,
          TiDBSchemaAdapter::localDateTimeToFlink,
          LocalTime.class,
          TiDBSchemaAdapter::localTimeToFlink);

  /** transform Row type to RowData type */
  public static Object toRowDataType(Object object) {
    if (object == null) {
      return null;
    }

    Class<?> clazz = object.getClass();
    if (!ROW_DATA_CONVERTERS.containsKey(clazz)) {
      return object;
    } else {
      return ROW_DATA_CONVERTERS.get(clazz).apply(object);
    }
  }

  /**
   * transform tidb java object to Flink java object by given tidb Datatype
   *
   * @param object TiKV java object
   * @param tidbType TiDB datatype
   */
  public static Object getObjectWithDataType(@Nullable Object object, TiDBType tidbType) {
    if (object == null) {
      return null;
    }
    switch (tidbType.getJavaType()) {
      case INTEGER:
      case BYTES:
      case SHORT:
      case FLOAT:
      case DOUBLE:
      case LONG:
      case BYTE:
      case BOOLEAN:
      case LOCALDATATIME:
        return object;
      case TIMESTAMP:
        return ((Timestamp) object).toLocalDateTime();
      case STRING:
        return StringData.fromString((String) object);
      case TIME:
        return ((Time) object).toLocalTime();
      case BIGINTEGER:
      case BIGDECIMAL:
        return DecimalData.fromBigDecimal(
            (BigDecimal) object, tidbType.getPrecision(), tidbType.getScale());
      case DATE:
        return ((Date) object).toLocalDate();
      default:
        throw new IllegalArgumentException(
            String.format(
                "Can not convert tidb data to flink data, object = %s, tidbType = %s",
                object, tidbType));
    }
  }

  /**
   * Converting tidb type to flink type, note that we will ignore the nullable attribute, this is to
   * skip some of the checks when inserting data
   *
   * @param tiDBType tidb type
   * @return Flink type
   */
  public static DataType toFlinkType(TiDBType tiDBType) {
    switch (tiDBType.getJavaType()) {
      case STRING:
        return DataTypes.STRING();
      case BYTES:
        return DataTypes.BYTES();
      case TIME:
        return DataTypes.TIME();
      case BIGINTEGER:
      case BIGDECIMAL:
        return DataTypes.DECIMAL(tiDBType.getPrecision(), tiDBType.getScale());
      case INTEGER:
        return DataTypes.INT();
      case SHORT:
        return DataTypes.SMALLINT();
      case BYTE:
        return DataTypes.TINYINT();
      case DATE:
        return DataTypes.DATE();
      case LONG:
        return DataTypes.BIGINT();
      case FLOAT:
        return DataTypes.FLOAT();
      case DOUBLE:
        return DataTypes.DOUBLE();
      case BOOLEAN:
        return DataTypes.BOOLEAN();
      case TIMESTAMP:
      case LOCALDATATIME:
        return DataTypes.TIMESTAMP();
      default:
        throw new IllegalArgumentException("Can not get Flink Type by TiDB type: " + tiDBType);
    }
  }

  public DataType getRowDataType() {
    return rowDataType;
  }

  public DataType getPhysicalRowDataType() {
    Field[] fields =
        IntStream.range(0, fieldNames.size())
            .filter(i -> !metadata.containsKey(fieldNames.get(i)))
            .mapToObj(i -> DataTypes.FIELD(fieldNames.get(i), fieldTypes.get(i)))
            .toArray(Field[]::new);
    return DataTypes.ROW(fields);
  }

  @SuppressWarnings("unchecked")
  public TypeInformation<RowData> getProducedType() {
    return (TypeInformation<RowData>)
        ScanRuntimeProviderContext.INSTANCE.createTypeInformation(rowDataType);
  }

  public TiDBMetadata[] getMetadata() {
    return metadata.values().toArray(new TiDBMetadata[0]);
  }

  public CDCMetadata[] getCDCMetadata() {
    return metadata.values().stream().map(TiDBMetadata::toCraft).toArray(CDCMetadata[]::new);
  }

  public CDCSchemaAdapter toCDCSchemaAdapter() {
    return new CDCSchemaAdapter(getPhysicalRowDataType(), getCDCMetadata());
  }

  public static LinkedHashMap<String, TiDBMetadata> parseMetadataColumns(
      Map<String, String> properties) {
    String metadataString = properties.get(METADATA_INCLUDED);
    if (StringUtils.isEmpty(metadataString)) {
      return new LinkedHashMap<>();
    }
    LinkedHashMap<String, TiDBMetadata> result = new LinkedHashMap<>();
    if (metadataString.equals(METADATA_INCLUDED_ALL)) {
      Arrays.stream(TiDBMetadata.values())
          .forEach(metadata -> result.put(metadata.getKey(), metadata));
      return result;
    }
    for (String pair : metadataString.split(",")) {
      String[] split = pair.split("=");
      if (split.length != 2) {
        throw new IllegalArgumentException("Error format for " + METADATA_INCLUDED);
      }
      String metadataName = split[0];
      String columnName = split[1];
      result.put(columnName, TiDBMetadata.fromKey(metadataName));
    }
    return result;
  }
}
