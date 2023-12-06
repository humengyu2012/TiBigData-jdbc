package io.tidb.bigdata.jdbc.core;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;

public class TiDBColumn {

  private final String name;
  private final JavaType javaType;
  private final int precision;
  private final int scale;
  private final boolean nullable;

  public TiDBColumn(String name, JavaType javaType, int precision, int scale, boolean nullable) {
    this.name = name;
    this.javaType = javaType;
    this.precision = precision;
    this.scale = scale;
    this.nullable = nullable;
  }

  public String getName() {
    return name;
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
    return "TiDBColumn{"
        + "name='"
        + name
        + '\''
        + ", javaType="
        + javaType
        + ", precision="
        + precision
        + ", scale="
        + scale
        + ", nullable="
        + nullable
        + '}';
  }

  public enum JavaType {
    BOOLEAN(Boolean.class),
    BYTE(Byte.class),
    BYTES(byte[].class),
    SHORT(Short.class),
    INTEGER(Integer.class),
    LONG(Long.class),
    FLOAT(Float.class),
    DOUBLE(Double.class),
    BIGINTEGER(BigInteger.class),
    BIGDECIMAL(BigDecimal.class),
    STRING(String.class),
    DATE(Date.class),
    TIME(Time.class),
    TIMESTAMP(Timestamp.class),
    LOCALDATATIME(LocalDateTime.class);

    private final Class<?> javaClass;

    JavaType(Class<?> javaClass) {
      this.javaClass = javaClass;
    }

    public Class<?> getJavaClass() {
      return javaClass;
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
