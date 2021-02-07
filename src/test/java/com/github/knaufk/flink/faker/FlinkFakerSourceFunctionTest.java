package com.github.knaufk.flink.faker;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.*;
import org.junit.jupiter.api.Test;

class FlinkFakerSourceFunctionTest {

  public static final LogicalType[] ALL_SUPPORTED_DATA_TYPES =
      new LogicalType[] {
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
        new BooleanType(),
        new TimestampType()
      };
  public static final String[] EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES =
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
        "#{date.past '15','5','SECONDS'}"
      };

  @Test
  public void testSimpleExpressions() throws Exception {

    String[] fieldExpressions = new String[] {"#{food.vegetables}", "#{Food.measurement_sizes}"};
    Float[] fieldNullRates = neverNull(2);

    LogicalType[] types = {new VarCharType(255), new VarCharType(Integer.MAX_VALUE)};
    FlinkFakerSourceFunction flinkFakerSourceFunction =
        new FlinkFakerSourceFunction(fieldExpressions, fieldNullRates, types, 100, 10);
    flinkFakerSourceFunction.open(new Configuration());

    assertThat(flinkFakerSourceFunction.generateNextRow().getArity()).isEqualTo(2);
    assertThat(flinkFakerSourceFunction.generateNextRow().getString(0)).isNotNull();
    assertThat(flinkFakerSourceFunction.generateNextRow().getString(1)).isNotNull();
  }

  @Test
  public void testREADME() throws Exception {
    String[] fieldExpressions =
        new String[] {
          "#{superhero.name}", "#{superhero.power}", "#{number.numberBetween '0','1000'}"
        };
    Float[] fieldNullRates = neverNull(3);

    LogicalType[] types = {
      new VarCharType(Integer.MAX_VALUE), new VarCharType(Integer.MAX_VALUE), new IntType()
    };
    FlinkFakerSourceFunction flinkFakerSourceFunction =
        new FlinkFakerSourceFunction(fieldExpressions, fieldNullRates, types, 100, 10);
    flinkFakerSourceFunction.open(new Configuration());

    RowData rowData = flinkFakerSourceFunction.generateNextRow();
    assertThat(rowData.getArity()).isEqualTo(3);
    for (int i = 0; i < fieldExpressions.length; i++) {
      assertThat(rowData.isNullAt(i)).isFalse();
    }
  }

  @Test
  public void testSupportedDataTypes() throws Exception {

    Float[] fieldNullRates = neverNull(12);

    FlinkFakerSourceFunction flinkFakerSourceFunction =
        new FlinkFakerSourceFunction(
            EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES,
            fieldNullRates,
            ALL_SUPPORTED_DATA_TYPES,
            100,
            10);
    flinkFakerSourceFunction.open(new Configuration());

    RowData rowData = flinkFakerSourceFunction.generateNextRow();
    assertThat(rowData.getArity()).isEqualTo(12);
    for (int i = 0; i < EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES.length; i++) {
      assertThat(rowData.isNullAt(i)).isFalse();
    }
  }

  @Test
  public void testNullsForAllDatatypes() throws Exception {
    Float[] fieldNullRates = alwaysNull(12);

    FlinkFakerSourceFunction flinkFakerSourceFunction =
        new FlinkFakerSourceFunction(
            EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES,
            fieldNullRates,
            ALL_SUPPORTED_DATA_TYPES,
            100,
            10);
    flinkFakerSourceFunction.open(new Configuration());

    RowData rowData = flinkFakerSourceFunction.generateNextRow();
    assertThat(rowData.getArity()).isEqualTo(12);
    for (int i = 0; i < EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES.length; i++) {
      assertThat(rowData.isNullAt(i)).isTrue();
    }
  }

  private Float[] neverNull(int size) {
    return getNullRates(size, 0.0f);
  }

  private Float[] alwaysNull(int size) {
    return getNullRates(size, 1.0f);
  }

  private Float[] getNullRates(int size, float v) {
    Float[] zeroNullRates = new Float[size];
    for (int i = 0; i < zeroNullRates.length; i++) {
      zeroNullRates[i] = v;
    }
    return zeroNullRates;
  }
}
