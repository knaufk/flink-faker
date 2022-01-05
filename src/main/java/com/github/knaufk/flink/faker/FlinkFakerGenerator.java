package com.github.knaufk.flink.faker;

import java.util.Random;
import net.datafaker.Faker;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

class FlinkFakerGenerator extends RichFlatMapFunction<Long, RowData> {

  private Faker faker;
  private Random rand;
  private final FieldInfo[] fieldInfos;
  private final long rowsPerSecond;
  private long soFarThisSecond;
  private long nextReadTime;;

  public FlinkFakerGenerator(FieldInfo[] fieldInfos, long rowsPerSecond) {
    this.fieldInfos = fieldInfos;
    this.rowsPerSecond = rowsPerSecond;
  }

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    faker = new Faker();
    rand = new Random();
    nextReadTime = System.currentTimeMillis();
    soFarThisSecond = 0;
  }

  @Override
  public void flatMap(Long trigger, Collector<RowData> collector) throws Exception {
    collector.collect(generateNextRow());
    recordAndMaybeRest();
  }

  private void recordAndMaybeRest() throws InterruptedException {
    soFarThisSecond++;
    if (soFarThisSecond >= getRowsPerSecondForSubTask()) {
      rest();
    }
  }

  private void rest() throws InterruptedException {
    nextReadTime += 1000;
    long toWaitMs = Math.max(0, nextReadTime - System.currentTimeMillis());
    Thread.sleep(toWaitMs);
    soFarThisSecond = 0;
  }

  @VisibleForTesting
  RowData generateNextRow() {
    GenericRowData row = new GenericRowData(fieldInfos.length);
    for (int i = 0; i < fieldInfos.length; i++) {
      row.setField(i, FakerUtils.stringValueToType(fieldInfos[i], f -> faker.expression(f)));
    }
    return row;
  }

  private long getRowsPerSecondForSubTask() {
    int numSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
    int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
    long baseRowsPerSecondPerSubtask = rowsPerSecond / numSubtasks;

    // Always emit at least one record per second per subtask so that each subtasks makes some
    // progress.
    // This ensure that the overall number of rows is correct and checkpointing works reliably.
    return Math.max(
        1,
        rowsPerSecond % numSubtasks > indexOfThisSubtask
            ? baseRowsPerSecondPerSubtask + 1
            : baseRowsPerSecondPerSubtask);
  }
}
