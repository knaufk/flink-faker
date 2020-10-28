package com.github.knaufk.flink.faker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

class FlinkFakerLookupFunctionTest {

  @Test
  public void testLookupWithMultipleKeys() throws Exception {

    ListOutputCollector collector = new ListOutputCollector();

    TableSchema schema =
        TableSchema.builder()
            .field("f0", DataTypes.TINYINT())
            .field("f1", DataTypes.SMALLINT())
            .field("f2", DataTypes.INT())
            .field("f3", DataTypes.BIGINT())
            .field("f4", DataTypes.DOUBLE())
            .field("f5", DataTypes.FLOAT())
            .field("f6", DataTypes.DECIMAL(6, 2))
            .field("f7", DataTypes.CHAR(10))
            .field("f8", DataTypes.VARCHAR(255))
            .field("f9", DataTypes.STRING())
            .field("f10", DataTypes.BOOLEAN())
            .build();

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
        new FlinkFakerLookupFunction(fieldExpressions, schema, keys);

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
