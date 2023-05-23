package com.github.knaufk.flink.faker;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.List;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.Test;

public class FlinkFakerIntegrationTest {

  public static final int NUM_ROWS = 5;

  @Test
  public void testRandomResultsWithoutParallelism() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    tEnv.executeSql(
        "CREATE TABLE server_logs ( \n"
            + "    client_ip STRING,\n"
            + "    client_identity STRING, \n"
            + "    userid STRING, \n"
            + "    request_line STRING, \n"
            + "    status_code STRING\n"
            + ") WITH (\n"
            + "  'connector' = 'faker', \n"
            + "  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',\n"
            + "  'fields.client_identity.expression' =  '-',\n"
            + "  'fields.userid.expression' =  '-',\n"
            + "  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\\.html|/login\\.html|/prod\\.html|cart\\.html|/order\\.html){1}''} #{regexify ''(HTTP/1\\.1|HTTP/2|/HTTP/1\\.0){1}''}',\n"
            + "  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}',\n"
            + "  'number-of-rows' = '2'\n"
            + ")");

    TableResult tableResult = tEnv.executeSql("SELECT * FROM server_logs");

    CloseableIterator<Row> collect = tableResult.collect();

    Row row1 = collect.next();
    Row row2 = collect.next();

    assertThat(row1).isNotEqualTo(row2);
  }

  @Test
  public void testAllPrimitiveDataTypes() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(8);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    tEnv.executeSql(
        "CREATE TABLE all_types ("
            + "f0 TINYINT, \n"
            + "f1 SMALLINT, \n"
            + "f2 INT, \n"
            + "f3 BIGINT, \n"
            + "f4 DOUBLE, \n"
            + "f5 FLOAT, \n"
            + "f6 DECIMAL(6,3), \n"
            + "f7 CHAR(10), \n"
            + "f8 VARCHAR(255), \n"
            + "f9 STRING, \n"
            + "f10 BOOLEAN \n"
            + ") WITH ( \n"
            + "'connector' = 'faker', \n"
            + "'number-of-rows' = '"
            + NUM_ROWS
            + "', \n"
            + "'fields.f0.expression' = '#{number.numberBetween ''-128'',''127''}', \n"
            + "'fields.f1.expression' = '#{number.numberBetween ''-32768'',''32767''}', \n"
            + "'fields.f2.expression' = '#{number.numberBetween ''-2147483648'',''2147483647''}', \n"
            + "'fields.f3.expression' = '#{number.randomNumber ''12'',''false''}', \n"
            + "'fields.f4.expression' = '#{number.randomDouble ''3'',''-999'',''999''}', \n"
            + "'fields.f5.expression' = '#{number.randomDouble ''3'',''-999'',''999''}', \n"
            + "'fields.f6.expression' = '#{number.randomDouble ''3'',''-999'',''999''}', \n"
            + "'fields.f7.expression' = '#{Lorem.characters ''10''}', \n"
            + "'fields.f8.expression' = '#{Lorem.characters ''255''}', \n"
            + "'fields.f9.expression' = '#{Lorem.sentence}', \n"
            + "'fields.f10.expression' = '#{regexify ''(true|false){1}''}' \n"
            + ");");

    TableResult tableResult = tEnv.executeSql("SELECT * FROM all_types");

    CloseableIterator<Row> collect = tableResult.collect();

    int numRows = 0;
    while (collect.hasNext()) {
      collect.next();
      numRows++;
    }

    assertThat(numRows).isEqualTo(NUM_ROWS);
  }

  @Test
  public void testWithComputedColumn() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(8);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    tEnv.executeSql(
        "CREATE TABLE server_logs ( \n"
            + "    client_ip STRING,\n"
            + "    client_identity STRING, \n"
            + "    userid STRING, \n"
            + "    log_time AS PROCTIME(),\n"
            + "    request_line STRING, \n"
            + "    status_code STRING\n"
            + ") WITH (\n"
            + "  'connector' = 'faker', \n"
            + "  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',\n"
            + "  'fields.client_identity.expression' =  '-',\n"
            + "  'fields.userid.expression' =  '-',\n"
            + "  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\\.html|/login\\.html|/prod\\.html|cart\\.html|/order\\.html){1}''} #{regexify ''(HTTP/1\\.1|HTTP/2|/HTTP/1\\.0){1}''}',\n"
            + "  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}',\n"
            + "  'number-of-rows' = '"
            + NUM_ROWS
            + "'\n"
            + ")");

    TableResult tableResult = tEnv.executeSql("SELECT * FROM server_logs");

    CloseableIterator<Row> collect = tableResult.collect();

    int numRows = 0;
    while (collect.hasNext()) {
      collect.next();
      numRows++;
    }

    assertThat(numRows).isEqualTo(NUM_ROWS);
  }

  @Test
  public void testFlinkFakerWithLimitedNumberOfRows() {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(8);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

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

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(8);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

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

  @Test
  public void testLimitPushDown() throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(8);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    tEnv.executeSql(
        "CREATE TEMPORARY TABLE faker_table (\n"
            + "	f0 INTEGER\n"
            + ") WITH (\n"
            + "	'connector' = 'faker',\n"
            + "	'fields.f0.expression' = '#{number.numberBetween ''10'',''100''}',\n"
            + " 'number-of-rows' = '100'\n"
            + ")");

    final Table table = tEnv.sqlQuery("SELECT * FROM faker_table LIMIT 10");
    String[] explain = table.explain().split("==.*==\\s+");
    assertThat(explain).hasSize(4);
    assertThat(explain[2])
        .contains(
            "table=[[default_catalog, default_database, faker_table, limit=[10]]], fields=[f0]");

    List<Row> rows = CollectionUtil.iteratorToList(table.execute().collect());
    assertThat(rows.size()).isEqualTo(10);
  }

  @Test
  public void testLocale() throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(8);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    tEnv.executeSql(
        "CREATE TEMPORARY TABLE faker_table (\n"
            + "	f0 STRING\n"
            + ") WITH (\n"
            + "	'connector' = 'faker',\n"
            + "	'fields.f0.expression' = '#{Name.firstname}',\n"
            + "	'fields.f0.locale' = 'ru-RU',\n"
            + " 'number-of-rows' = '3'\n"
            + ")");

    TableResult tableResult = tEnv.executeSql("SELECT * FROM faker_table");

    CloseableIterator<Row> collect = tableResult.collect();

    int numRows = 0;
    while (collect.hasNext()) {
      Row row = collect.next();
      // no assertions on map sizes, it may differ from given length due to duplicates
      assertThat(
              row.getField("f0")
                  .toString()
                  .chars()
                  .allMatch(
                      t -> (Character.UnicodeBlock.of(t).equals(Character.UnicodeBlock.CYRILLIC))))
          .isTrue();
      numRows++;
    }

    assertThat(numRows).isEqualTo(3);
  }
}
