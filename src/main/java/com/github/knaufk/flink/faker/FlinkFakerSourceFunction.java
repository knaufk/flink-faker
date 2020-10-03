package com.github.knaufk.flink.faker;

import com.github.javafaker.Faker;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

public class FlinkFakerSourceFunction extends RichParallelSourceFunction<RowData> {

  private volatile boolean cancelled;
  private Faker faker;

  private String[] fieldExpressions;

  public FlinkFakerSourceFunction(String[] fieldExpressions) {
    this.fieldExpressions = fieldExpressions;
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
      row.setField(i, StringData.fromString(faker.expression(fieldExpressions[i])));
    }
    return row;
  }
}
