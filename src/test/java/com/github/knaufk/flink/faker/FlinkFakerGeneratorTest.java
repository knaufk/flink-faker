package com.github.knaufk.flink.faker;

import static com.github.knaufk.flink.faker.TestUtils.*;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.*;
import org.junit.jupiter.api.Test;

class FlinkFakerGeneratorTest {

  @Test
  public void testSimpleExpressions() throws Exception {

    String[][] fieldExpressions =
        new String[][] {{"#{food.vegetables}"}, {"#{Food.measurement_sizes}"}};

    LogicalType[] types = {new VarCharType(255), new VarCharType(Integer.MAX_VALUE)};
    FlinkFakerGenerator flinkFakerSourceFunction =
        new FlinkFakerGenerator(
            TestUtils.constructFieldInfos(fieldExpressions, types, neverNull(2), getArrayOfOnes(2)),
            100);
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
    FlinkFakerGenerator flinkFakerSourceFunction =
        new FlinkFakerGenerator(
            TestUtils.constructFieldInfos(fieldExpressions, types, neverNull(3), getArrayOfOnes(3)),
            100);
    flinkFakerSourceFunction.open(new Configuration());

    RowData rowData = flinkFakerSourceFunction.generateNextRow();
    assertThat(rowData.getArity()).isEqualTo(3);
    for (int i = 0; i < fieldExpressions.length; i++) {
      assertThat(rowData.isNullAt(i)).isFalse();
    }
  }

  @Test
  public void testSupportedDataTypes() throws Exception {

    FlinkFakerGenerator flinkFakerSourceFunction =
        new FlinkFakerGenerator(
            TestUtils.constructFieldInfos(
                EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES,
                ALL_SUPPORTED_DATA_TYPES,
                neverNull(EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES.length),
                getArrayOfOnes(EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES.length)),
            100);
    flinkFakerSourceFunction.open(new Configuration());

    RowData rowData = flinkFakerSourceFunction.generateNextRow();
    assertThat(rowData.getArity()).isEqualTo(EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES.length);
    for (int i = 0; i < EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES.length; i++) {
      assertThat(rowData.isNullAt(i)).isFalse();
    }
  }

  @Test
  public void testNullsForAllDatatypes() throws Exception {

    FlinkFakerGenerator flinkFakerSourceFunction =
        new FlinkFakerGenerator(
            TestUtils.constructFieldInfos(
                EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES,
                ALL_SUPPORTED_DATA_TYPES,
                alwaysNull(EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES.length),
                getArrayOfOnes(EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES.length)),
            100);
    flinkFakerSourceFunction.open(new Configuration());

    RowData rowData = flinkFakerSourceFunction.generateNextRow();
    assertThat(rowData.getArity()).isEqualTo(EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES.length);
    for (int i = 0; i < EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES.length; i++) {
      assertThat(rowData.isNullAt(i)).isTrue();
    }
  }
}
