package com.github.knaufk.flink.faker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

class FlinkFakerLookupFunctionTest {

  @Test
  public void testLookupWithMultipleKeys() throws Exception {

    ListOutputCollector collector = new ListOutputCollector();

    LogicalType[] types = {
      new TinyIntType(),
      new SmallIntType(),
      new IntType(),
      new BigIntType(),
      new DoubleType(),
      new FloatType(),
      new DecimalType(6, 2),
      new CharType(10),
      new VarCharType(255),
      new VarCharType(Integer.MAX_VALUE),
      new BooleanType()
    };

    String[] fieldExpressions =
        new String[] {
          "#{number.numberBetween '-128','127'}",
          "#{number.numberBetween '-32768','32767'}",
          "#{number.numberBetween '-2147483648','2147483647'}",
          "#{number.randomNumber '12','false'}",
          "#{number.randomDouble '3','-1000','1000'}",
          "#{number.randomDouble '3','-1000','1000'}",
          "#{number.randomDouble '3','-1000','1000'}",
          "#{Lorem.characters '10'}",
          "#{Lorem.characters '255'}",
          "#{Lorem.sentence}",
          "#{regexify '(true|false){1}'}",
        };

    int[][] keys = {{1, 0}, {2, 0}};

    FlinkFakerLookupFunction flinkFakerLookupFunction =
        new FlinkFakerLookupFunction(fieldExpressions, types, keys);

    flinkFakerLookupFunction.setCollector(collector);
    flinkFakerLookupFunction.open(null);
    flinkFakerLookupFunction.eval(10, 11);

    RowData rowData = collector.getOutputs().get(0);

    assertThat(rowData.getInt(1)).isEqualTo(10);
    assertThat(rowData.getInt(2)).isEqualTo(11);

    for (int i = 0; i < fieldExpressions.length; i++) {
      assertThat(rowData.isNullAt(i)).isFalse();
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
