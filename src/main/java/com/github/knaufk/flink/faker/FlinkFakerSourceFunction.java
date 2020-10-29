package com.github.knaufk.flink.faker;

import com.github.javafaker.Faker;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

public class FlinkFakerSourceFunction extends RichParallelSourceFunction<RowData> {

  private volatile boolean cancelled;
  private Faker faker;

  private String[] fieldExpressions;
  private LogicalType[] types;

  public FlinkFakerSourceFunction(String[] fieldExpressions, LogicalType[] types) {
    this.fieldExpressions = fieldExpressions;
    this.types = types;
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
    for (int i = 0; i < fieldExpressions.length; i++) {
      LogicalTypeRoot typeRoot = (types[i]).getTypeRoot();
      String value = faker.expression(fieldExpressions[i]);
      row.setField(i, FakerUtils.stringValueToType(value, typeRoot));
    }
    return row;
  }
}
