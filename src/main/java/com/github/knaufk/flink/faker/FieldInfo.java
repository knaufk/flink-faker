package com.github.knaufk.flink.faker;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import org.apache.flink.table.types.logical.LogicalType;

public class FieldInfo implements Serializable {
  private final Float nullRate;
  private final LogicalType logicalType;
  private final String[] expressions;
  private final Integer length;
  private final Map<String, FieldInfo> nestedFields;

  public FieldInfo(
      Float nullRate,
      LogicalType logicalType,
      String[] expressions,
      Integer length,
      Map<String, FieldInfo> nestedFields) {
    this.nullRate = nullRate;
    this.logicalType = logicalType;
    this.expressions = expressions;
    this.length = length;
    this.nestedFields = nestedFields;
  }

  public Float getNullRate() {
    return nullRate;
  }

  public String[] getExpressions() {
    return expressions;
  }

  public Integer getLength() {
    return length;
  }

  public LogicalType getLogicalType() {
    return logicalType;
  }

  public Map<String, FieldInfo> getNestedFields() {
    return nestedFields;
  }

  @Override
  public String toString() {
    return "FieldInfo{"
        + "nullRate="
        + nullRate
        + ", logicalType="
        + logicalType
        + ", expressions="
        + Arrays.toString(expressions)
        + ", length="
        + length
        + ", nestedFields="
        + nestedFields
        + '}';
  }
}
