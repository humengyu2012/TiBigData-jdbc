package io.tidb.bigdata.jdbc.core;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;

public class TiDBColumn implements Serializable {

  private final String name;
  private final TiDBType type;

  public TiDBColumn(String name, TiDBType type) {
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public TiDBType getType() {
    return type;
  }

  @Override
  public String toString() {
    return "TiDBColumn{" + "name='" + name + '\'' + ", type=" + type + '}';
  }

  public static class TiDBType implements Serializable {

    private final JavaType javaType;
    private final int precision;
    private final int scale;
    private final boolean nullable;

    public TiDBType(JavaType javaType, int precision, int scale, boolean nullable) {
      this.javaType = javaType;
      this.precision = precision;
      this.scale = scale;
      this.nullable = nullable;
    }

    public JavaType getJavaType() {
      return javaType;
    }

    public int getPrecision() {
      return precision;
    }

    public int getScale() {
      return scale;
    }

    public boolean isNullable() {
      return nullable;
    }

    @Override
    public String toString() {
      return "TiDBType{"
          + "javaType="
          + javaType
          + ", precision="
          + precision
          + ", scale="
          + scale
          + ", nullable="
          + nullable
          + '}';
    }
  }

  public interface ResultSetValueProvider {

    Object getValue(ResultSet resultSet, int index) throws SQLException;
  }

  public enum JavaType implements Serializable {
    BOOLEAN(Boolean.class, ResultSet::getBoolean),
    BYTE(Byte.class, ResultSet::getByte),
    BYTES(byte[].class, ResultSet::getBytes),
    SHORT(Short.class, ResultSet::getShort),
    INTEGER(Integer.class, ResultSet::getInt),
    LONG(Long.class, ResultSet::getLong),
    FLOAT(Float.class, ResultSet::getFloat),
    DOUBLE(Double.class, ResultSet::getDouble),
    BIGINTEGER(BigInteger.class, ResultSet::getObject),
    BIGDECIMAL(BigDecimal.class, ResultSet::getBigDecimal),
    STRING(String.class, ResultSet::getString),
    DATE(Date.class, ResultSet::getDate),
    TIME(Time.class, ResultSet::getTime),
    TIMESTAMP(Timestamp.class, ResultSet::getTimestamp),
    LOCALDATATIME(LocalDateTime.class, ResultSet::getObject);

    private final Class<?> javaClass;
    // get value from ressult
    private final ResultSetValueProvider valueProvider;

    JavaType(Class<?> javaClass, ResultSetValueProvider valueProvider) {
      this.javaClass = javaClass;
      this.valueProvider = valueProvider;
    }

    public Class<?> getJavaClass() {
      return javaClass;
    }

    public ResultSetValueProvider getValueProvider() {
      return valueProvider;
    }

    public static JavaType fromClass(Class<?> javaClass) {
      return Arrays.stream(values())
          .filter(value -> value.getJavaClass().equals(javaClass))
          .findFirst()
          .orElseThrow(
              () -> new IllegalArgumentException("Unsupported class: " + javaClass.getName()));
    }

    public static JavaType fromClassName(String className) {
      return Arrays.stream(values())
          .filter(value -> value.getJavaClass().getName().equals(className))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("Unsupported class: " + className));
    }
  }
}
