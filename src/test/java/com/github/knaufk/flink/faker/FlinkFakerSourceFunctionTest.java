package com.github.knaufk.flink.faker;

import static com.github.knaufk.flink.faker.TestUtils.*;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.*;
import org.junit.jupiter.api.Test;

class FlinkFakerSourceFunctionTest {

  @Test
  public void testSimpleExpressions() throws Exception {

    String[][] fieldExpressions =
        new String[][] {{"#{food.vegetables}"}, {"#{Food.measurement_sizes}"}};

    LogicalType[] types = {new VarCharType(255), new VarCharType(Integer.MAX_VALUE)};
    FlinkFakerSourceFunction flinkFakerSourceFunction =
        new FlinkFakerSourceFunction(
            fieldExpressions, neverNull(2), getArrayOfOnes(2), types, 100, 10);
    flinkFakerSourceFunction.open(new Configuration());

    assertThat(flinkFakerSourceFunction.generateNextRow().getArity()).isEqualTo(2);
    assertThat(flinkFakerSourceFunction.generateNextRow().getString(0)).isNotNull();
    assertThat(flinkFakerSourceFunction.generateNextRow().getString(1)).isNotNull();
  }

  @Test
  public void testREADME() throws Exception {
    String[][] fieldExpressions =
        new String[][] {
          {"#{superhero.name}"}, {"#{superhero.power}"}, {"#{number.numberBetween '0','1000'}"}
        };

    LogicalType[] types = {
      new VarCharType(Integer.MAX_VALUE), new VarCharType(Integer.MAX_VALUE), new IntType()
    };
    FlinkFakerSourceFunction flinkFakerSourceFunction =
        new FlinkFakerSourceFunction(
            fieldExpressions, neverNull(3), getArrayOfOnes(3), types, 100, 10);
    flinkFakerSourceFunction.open(new Configuration());

    RowData rowData = flinkFakerSourceFunction.generateNextRow();
    assertThat(rowData.getArity()).isEqualTo(3);
    for (int i = 0; i < fieldExpressions.length; i++) {
      assertThat(rowData.isNullAt(i)).isFalse();
    }
  }

  @Test
  public void testSupportedDataTypes() throws Exception {

    FlinkFakerSourceFunction flinkFakerSourceFunction =
        new FlinkFakerSourceFunction(
            EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES,
            neverNull(EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES.length),
            getArrayOfOnes(EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES.length),
            ALL_SUPPORTED_DATA_TYPES,
            100,
            10);
    flinkFakerSourceFunction.open(new Configuration());

    RowData rowData = flinkFakerSourceFunction.generateNextRow();
    assertThat(rowData.getArity()).isEqualTo(EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES.length);
    for (int i = 0; i < EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES.length; i++) {
      assertThat(rowData.isNullAt(i)).isFalse();
    }
  }

  @Test
  public void testNullsForAllDatatypes() throws Exception {

    FlinkFakerSourceFunction flinkFakerSourceFunction =
        new FlinkFakerSourceFunction(
            EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES,
            alwaysNull(EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES.length),
            getArrayOfOnes(EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES.length),
            ALL_SUPPORTED_DATA_TYPES,
            100,
            10);
    flinkFakerSourceFunction.open(new Configuration());

    RowData rowData = flinkFakerSourceFunction.generateNextRow();
    assertThat(rowData.getArity()).isEqualTo(EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES.length);
    for (int i = 0; i < EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES.length; i++) {
      assertThat(rowData.isNullAt(i)).isTrue();
    }
  }
}
