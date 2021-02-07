package com.github.knaufk.flink.faker;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

public class FlinkFakerIntegrationTest {

  public static final int NUM_ROWS = 10;

  @Test
  public void testFlinkFakerWithLimitedNumberOfRows() {

    EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
    TableEnvironment tEnv = TableEnvironment.create(settings);

    tEnv.executeSql(
        "CREATE TEMPORARY TABLE heros (\n"
            + "  name STRING,\n"
            + "  `power` STRING, \n"
            + "  age INT\n"
            + ") WITH (\n"
            + "  'connector' = 'faker', \n"
            + "  'fields.name.expression' = '#{superhero.name}',\n"
            + "  'fields.power.expression' = '#{superhero.power}',\n"
            + "  'fields.power.null-rate' = '0.5',\n"
            + "  'fields.age.expression' = '#{number.numberBetween ''0'',''1000''}',\n"
            + "  'number-of-rows' = '"
            + NUM_ROWS
            + "'\n"
            + ")");

    TableResult tableResult = tEnv.executeSql("SELECT * FROM heros");

    CloseableIterator<Row> collect = tableResult.collect();

    int numRows = 0;
    while (collect.hasNext()) {
      collect.next();
      numRows++;
    }

    assertThat(numRows).isEqualTo(NUM_ROWS);
  }
}
