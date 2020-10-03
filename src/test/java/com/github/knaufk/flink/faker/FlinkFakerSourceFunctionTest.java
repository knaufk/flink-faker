package com.github.knaufk.flink.faker;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

class FlinkFakerSourceFunctionTest {

  @Test
  public void testSimpleExpressions() throws Exception {

    String[] fieldExpressions = new String[] {"#{food.vegetables}", "#{Food.measurement_sizes}"};
    FlinkFakerSourceFunction flinkFakerSourceFunction =
        new FlinkFakerSourceFunction(fieldExpressions);
    flinkFakerSourceFunction.open(new Configuration());

    assertThat(flinkFakerSourceFunction.generateNextRow().getArity()).isEqualTo(2);
    assertThat(flinkFakerSourceFunction.generateNextRow().getString(0)).isNotNull();
    assertThat(flinkFakerSourceFunction.generateNextRow().getString(1)).isNotNull();
  }

  @Test
  public void testIntegerExpression() throws Exception {

    String[] fieldExpressions = new String[] {"#{number.randomDigit}"};
    FlinkFakerSourceFunction flinkFakerSourceFunction =
        new FlinkFakerSourceFunction(fieldExpressions);
    flinkFakerSourceFunction.open(new Configuration());

    assertThat(flinkFakerSourceFunction.generateNextRow().getArity()).isEqualTo(1);
    assertThat(flinkFakerSourceFunction.generateNextRow().getString(0)).isNotNull();
  }
}
