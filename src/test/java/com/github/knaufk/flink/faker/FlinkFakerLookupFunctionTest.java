package com.github.knaufk.flink.faker;

import static com.github.knaufk.flink.faker.TestUtils.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

class FlinkFakerLookupFunctionTest {

  @Test
  public void testLookupWithMultipleKeys() throws Exception {

    ListOutputCollector collector = new ListOutputCollector();

    int[][] keys = {{1, 0}, {2, 0}};

    FlinkFakerLookupFunction flinkFakerLookupFunction =
        new FlinkFakerLookupFunction(
            EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES,
            neverNull(16),
            getArrayOfOnes(16),
            ALL_SUPPORTED_DATA_TYPES,
            keys);

    flinkFakerLookupFunction.setCollector(collector);
    flinkFakerLookupFunction.open(null);
    flinkFakerLookupFunction.eval(10, 11);

    RowData rowData = collector.getOutputs().get(0);

    assertThat(rowData.getInt(1)).isEqualTo(10);
    assertThat(rowData.getInt(2)).isEqualTo(11);

    for (int i = 0; i < EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES.length; i++) {
      assertThat(rowData.isNullAt(i)).isFalse();
    }
  }

  @Test
  public void testLookupWithMultipleKeysButOnlyNullValues() throws Exception {

    ListOutputCollector collector = new ListOutputCollector();

    int[][] keys = {{1, 0}, {2, 0}};

    FlinkFakerLookupFunction flinkFakerLookupFunction =
        new FlinkFakerLookupFunction(
            EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES,
            alwaysNull(16),
            getArrayOfOnes(16),
            ALL_SUPPORTED_DATA_TYPES,
            keys);

    flinkFakerLookupFunction.setCollector(collector);
    flinkFakerLookupFunction.open(null);
    flinkFakerLookupFunction.eval(10, 11);

    RowData rowData = collector.getOutputs().get(0);

    assertThat(rowData.getInt(1)).isEqualTo(10);
    assertThat(rowData.getInt(2)).isEqualTo(11);

    for (int i = 0; i < EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES.length; i++) {
      // key fields are not null, they are always equal to the value that was looked up
      if (i != 1 && i != 2) {
        assertThat(rowData.isNullAt(i)).isTrue();
      }
    }
  }

  private static final class ListOutputCollector implements Collector<RowData> {

    private final List<RowData> output = new ArrayList<>();

    @Override
    public void collect(RowData row) {
      this.output.add(row);
    }

    @Override
    public void close() {}

    public List<RowData> getOutputs() {
      return output;
    }
  }
}
