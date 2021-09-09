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

  @Test
  public void testFlinkFakerWithComplexTypes() {
    // test

    EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
    TableEnvironment tEnv = TableEnvironment.create(settings);

    tEnv.executeSql(
        "CREATE TEMPORARY TABLE hp (\n"
            + "  `character-with-age` MAP<STRING,INT>, \n"
            + "  `spells` MULTISET<STRING>, \n"
            + "  `locations` ARRAY<STRING>, \n"
            + "  `house-points` ROW<`house` STRING, `points` INT> \n"
            + ") WITH (\n"
            + "  'connector' = 'faker', \n"
            + "  'fields.character-with-age.key.expression' = '#{harry_potter.character}',\n"
            + "  'fields.character-with-age.value.expression' = '#{number.numberBetween ''10'',''100''}',\n"
            + "  'fields.character-with-age.length' = '2',\n"
            + "  'fields.spells.expression' = '#{harry_potter.spell}',\n"
            + "  'fields.spells.length' = '5',\n"
            + "  'fields.locations.expression' = '#{harry_potter.location}',\n"
            + "  'fields.locations.length' = '3',\n"
            + "  'fields.house-points.house.expression' = '#{harry_potter.house}',\n"
            + "  'fields.house-points.points.expression' = '#{number.numberBetween ''10'',''100''}',\n"
            + "  'number-of-rows' = '3'\n"
            + ")");

    TableResult tableResult = tEnv.executeSql("SELECT * FROM hp");

    CloseableIterator<Row> collect = tableResult.collect();

    int numRows = 0;
    while (collect.hasNext()) {
      Row row = collect.next();
      // no assertions on map sizes, it may differ from given length due to duplicates
      assertThat(row.getArity()).isEqualTo(4);
      assertThat(((String[]) row.getField("locations")).length).isEqualTo(3);
      assertThat(((Row) row.getField("house-points")).getArity()).isEqualTo(2);
      numRows++;
    }

    assertThat(numRows).isEqualTo(3);
  }
}
