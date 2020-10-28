package com.github.knaufk.flink.faker;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

class FlinkFakerSourceFunctionTest {

  @Test
  public void testSimpleExpressions() throws Exception {

    TableSchema schema =
        TableSchema.builder()
            .field("f0", DataTypes.VARCHAR(255))
            .field("f1", DataTypes.STRING())
            .build();

    String[] fieldExpressions = new String[] {"#{food.vegetables}", "#{Food.measurement_sizes}"};
    FlinkFakerSourceFunction flinkFakerSourceFunction =
        new FlinkFakerSourceFunction(fieldExpressions, schema);
    flinkFakerSourceFunction.open(new Configuration());

    assertThat(flinkFakerSourceFunction.generateNextRow().getArity()).isEqualTo(2);
    assertThat(flinkFakerSourceFunction.generateNextRow().getString(0)).isNotNull();
    assertThat(flinkFakerSourceFunction.generateNextRow().getString(1)).isNotNull();
  }

  @Test
  public void testREADME() throws Exception {
    TableSchema schema =
        TableSchema.builder()
            .field("name", DataTypes.STRING())
            .field("power", DataTypes.STRING())
            .field("age", DataTypes.INT())
            .build();

    String[] fieldExpressions =
        new String[] {
          "#{superhero.name}", "#{superhero.power}", "#{number.numberBetween '0','1000'}"
        };

    FlinkFakerSourceFunction flinkFakerSourceFunction =
        new FlinkFakerSourceFunction(fieldExpressions, schema);
    flinkFakerSourceFunction.open(new Configuration());

    RowData rowData = flinkFakerSourceFunction.generateNextRow();
    assertThat(rowData.getArity()).isEqualTo(3);
    for (int i = 0; i < fieldExpressions.length; i++) {
      assertThat(rowData.isNullAt(i)).isFalse();
    }
  }

  @Test
  public void testSupportedDataTypes() throws Exception {

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
    FlinkFakerSourceFunction flinkFakerSourceFunction =
        new FlinkFakerSourceFunction(fieldExpressions, schema);
    flinkFakerSourceFunction.open(new Configuration());

    RowData rowData = flinkFakerSourceFunction.generateNextRow();
    assertThat(rowData.getArity()).isEqualTo(11);
    for (int i = 0; i < fieldExpressions.length; i++) {
      assertThat(rowData.isNullAt(i)).isFalse();
    }
  }
}
