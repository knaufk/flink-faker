package com.github.knaufk.flink.faker;

import com.github.javafaker.Faker;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

public class FlinkFakerSourceFunction extends RichParallelSourceFunction<RowData> {

  private volatile boolean cancelled;
  private Faker faker;

  private String[] fieldExpressions;
  private TableSchema schema;

  public FlinkFakerSourceFunction(String[] fieldExpressions, TableSchema schema) {
    this.fieldExpressions = fieldExpressions;
    this.schema = schema;
  }

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    faker = new Faker();
  }

  @Override
  public void run(final SourceContext<RowData> sourceContext) throws Exception {
    while (!cancelled) {
      sourceContext.collect(generateNextRow());
    }
  }

  @Override
  public void cancel() {
    cancelled = true;
  }

  @VisibleForTesting
  RowData generateNextRow() {
    GenericRowData row = new GenericRowData(fieldExpressions.length);
    DataType[] fieldDataTypes = schema.getFieldDataTypes();
    for (int i = 0; i < fieldExpressions.length; i++) {
      DataType fieldDataType = fieldDataTypes[i];
      LogicalTypeRoot logicalType = fieldDataType.getLogicalType().getTypeRoot();
      String value = faker.expression(fieldExpressions[i]);
      row.setField(i, stringValueToType(value, logicalType));
    }
    return row;
  }

  private Object stringValueToType(String value, LogicalTypeRoot logicalType) {

    switch (logicalType) {
      case CHAR:
        return StringData.fromString(value);
      case VARCHAR:
        return StringData.fromString(value);
      case BOOLEAN:
        return Boolean.parseBoolean(value);
      case DECIMAL:
        return new BigDecimal(value);
      case TINYINT:
        return Byte.parseByte(value);
      case SMALLINT:
        return Short.parseShort(value);
      case INTEGER:
        return Integer.parseInt(value);
      case BIGINT:
        return new BigInteger(value);
      case FLOAT:
        return Float.parseFloat(value);
      case DOUBLE:
        return Double.parseDouble(value);
        //      case DATE:
        //        break;
        //      case TIME_WITHOUT_TIME_ZONE:
        //        break;
        //      case TIMESTAMP_WITHOUT_TIME_ZONE:
        //        break;
        //      case TIMESTAMP_WITH_TIME_ZONE:
        //        break;
        //      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        //        break;
        //      case INTERVAL_YEAR_MONTH:
        //        break;
        //      case INTERVAL_DAY_TIME:
        //        break;
        //      case ARRAY:
        //        break;
        //      case MULTISET:
        //        break;
        //      case MAP:
        //        break;
        //      case ROW:
        //        break;
        //      case DISTINCT_TYPE:
        //        break;
        //      case STRUCTURED_TYPE:
        //        break;
        //      case NULL:
        //        break;
        //      case RAW:
        //        break;
        //      case SYMBOL:
        //        break;
        //      case UNRESOLVED:
        //        break;
      default:
        throw new RuntimeException("Unsupported Data Type");
    }
  }
}
