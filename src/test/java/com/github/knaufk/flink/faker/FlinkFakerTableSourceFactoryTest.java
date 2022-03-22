package com.github.knaufk.flink.faker;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.jupiter.api.Test;

class FlinkFakerTableSourceFactoryTest {

  private static final Schema VALID_SCHEMA =
      Schema.newBuilder()
          .column("f0", DataTypes.TINYINT())
          .column("f1", DataTypes.SMALLINT())
          .column("f2", DataTypes.INT())
          .column("f3", DataTypes.BIGINT())
          .column("f4", DataTypes.DOUBLE())
          .column("f5", DataTypes.FLOAT())
          .column("f6", DataTypes.DECIMAL(6, 2))
          .column("f7", DataTypes.CHAR(10))
          .column("f8", DataTypes.VARCHAR(255))
          .column("f9", DataTypes.STRING())
          .column("f10", DataTypes.BOOLEAN())
          .column("f11", DataTypes.ARRAY(DataTypes.INT()))
          .column("f12", DataTypes.MAP(DataTypes.INT(), DataTypes.VARCHAR(255)))
          .column("f13", DataTypes.ROW(DataTypes.FIELD("age", DataTypes.INT())))
          .column("f14", DataTypes.MULTISET(DataTypes.CHAR(10)))
          .build();

  private static final Schema INVALID_SCHEMA =
      Schema.newBuilder()
          .column("f0", DataTypes.STRING())
          .column("f1", DataTypes.VARCHAR(100))
          .column("f2", DataTypes.NULL())
          .build();

  private static final Schema TINY_SCHEMA =
      Schema.newBuilder().column("f0", DataTypes.TINYINT()).build();

  @Test
  public void testSchemaWithNonSupportedTypesIsInvalid() {

    assertThatExceptionOfType(ValidationException.class)
        .isThrownBy(
            () -> {
              Map<String, String> properties = new HashMap();
              properties.put(FactoryUtil.CONNECTOR.key(), "faker");
              properties.put("fields.f0.expression", "#{number.numberBetween '-128','127'}");
              properties.put("fields.f1.expression", "#{number.numberBetween '-32768','32767'}");
              properties.put(
                  "fields.f2.expression", "#{number.numberBetween '-2147483648','2147483647'}");
              properties.put("fields.f3.expression", "#{number.randomNumber '12','false'}");
              properties.put("fields.f4.expression", "#{number.randomDouble '3','-1000','1000'}");
              properties.put("fields.f5.expression", "#{number.randomDouble '3','-1000','1000'}");
              properties.put("fields.f6.expression", "#{number.randomDouble '3','-1000','1000'}");
              properties.put("fields.f7.expression", "#{Lorem.characters '10'}");
              properties.put("fields.f8.expression", "#{Lorem.characters '255'}");
              properties.put("fields.f9.expression", "#{Lorem.sentence}");
              properties.put("fields.f10.expression", "#{regexify '(true|false){1}'}");
              createTableSource(properties, INVALID_SCHEMA);
            })
        .withStackTraceContaining("f2 is NULL.");
  }

  @Test
  public void testValidNullRateIsValid() {
    Map<String, String> properties = new HashMap();
    properties.put(FactoryUtil.CONNECTOR.key(), "faker");
    properties.put("fields.f0.expression", "#{number.numberBetween '-128','127'}");
    properties.put("fields.f0.null-rate", "0.1");

    createTableSource(properties, TINY_SCHEMA);
  }

  @Test
  public void testNegativeNullRateIsInvalid() {

    assertThatExceptionOfType(ValidationException.class)
        .isThrownBy(
            () -> {
              Map<String, String> properties = new HashMap();
              properties.put(FactoryUtil.CONNECTOR.key(), "faker");
              properties.put("fields.f0.expression", "#{number.numberBetween '-128','127'}");
              properties.put("fields.f0.null-rate", "-0.8");

              createTableSource(properties, TINY_SCHEMA);
            })
        .withStackTraceContaining("needs to be in [0,1]");
  }

  @Test
  public void testNullRateGreaterOneIsInvalid() {

    assertThatExceptionOfType(ValidationException.class)
        .isThrownBy(
            () -> {
              Map<String, String> properties = new HashMap();
              properties.put(FactoryUtil.CONNECTOR.key(), "faker");
              properties.put("fields.f0.expression", "#{number.numberBetween '-128','127'}");
              properties.put("fields.f0.null-rate", "1.01");

              createTableSource(properties, TINY_SCHEMA);
            })
        .withStackTraceContaining("needs to be in [0,1]");
  }

  @Test
  public void testPropertiesWithoutExpressionForOnecolumnIsInvalid() {

    assertThatExceptionOfType(ValidationException.class)
        .isThrownBy(
            () -> {
              Map<String, String> properties = new HashMap();
              properties.put(FactoryUtil.CONNECTOR.key(), "faker");
              properties.put("fields.f0.expression", "#{number.randomDigit}");
              properties.put("fields.f1.expression", "#{number.randomDigit}");

              createTableSource(properties, VALID_SCHEMA);
            })
        .withStackTraceContaining("No expression found for f2.");
  }

  @Test
  public void testInvalidExpressionIsInvalid() {

    assertThatExceptionOfType(ValidationException.class)
        .isThrownBy(
            () -> {
              Map<String, String> properties = new HashMap();
              properties.put(FactoryUtil.CONNECTOR.key(), "faker");
              properties.put("fields.f0.expression", "#{number.abc}");
              properties.put("fields.f1.expression", "#{number.randomDigit}");

              createTableSource(properties, VALID_SCHEMA);
            })
        .withStackTraceContaining("Invalid expression for column \"f0\".");
  }

  @Test
  public void testValidTableSourceIsValid() {

    Map<String, String> properties = new HashMap();
    properties.put(FactoryUtil.CONNECTOR.key(), "faker");
    properties.put("fields.f0.expression", "#{number.numberBetween '-128','127'}");
    properties.put("fields.f1.expression", "#{number.numberBetween '-32768','32767'}");
    properties.put("fields.f2.expression", "#{number.numberBetween '-2147483648','2147483647'}");
    properties.put("fields.f3.expression", "#{number.randomNumber '12','false'}");
    properties.put("fields.f4.expression", "#{number.randomDouble '3','-1000','1000'}");
    properties.put("fields.f5.expression", "#{number.randomDouble '3','-1000','1000'}");
    properties.put("fields.f6.expression", "#{number.randomDouble '3','-1000','1000'}");
    properties.put("fields.f7.expression", "#{Lorem.characters '10'}");
    properties.put("fields.f8.expression", "#{Lorem.characters '255'}");
    properties.put("fields.f9.expression", "#{Lorem.sentence}");
    properties.put("fields.f10.expression", "#{regexify '(true|false){1}'}");
    properties.put("fields.f11.expression", "#{number.numberBetween '-32768','32767'}");
    properties.put("fields.f12.key.expression", "#{number.numberBetween '-32768','32767'}");
    properties.put("fields.f12.value.expression", "#{Lorem.characters '255'}");
    properties.put("fields.f13.age.expression", "#{number.numberBetween '-32768','32767'}");
    properties.put("fields.f14.expression", "#{Lorem.characters '10'}");

    createTableSource(properties, VALID_SCHEMA);
  }

  @Test
  public void testTimestamps() {

    Map<String, String> properties = new HashMap();
    properties.put(FactoryUtil.CONNECTOR.key(), "faker");

    properties.put("fields.f0.expression", "#{date.past '15','SECONDS'}");
    properties.put("fields.f1.expression", "#{date.past '15','SECONDS'}");
    properties.put("fields.f2.expression", "#{date.past '15','SECONDS'}");

    Schema schema =
        Schema.newBuilder()
            .column("f0", DataTypes.TIMESTAMP())
            .column("f1", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE())
            .column("f2", DataTypes.TIMESTAMP_WITH_TIME_ZONE())
            .build();

    createTableSource(properties, schema);
  }

  private DynamicTableSource createTableSource(Map<String, String> properties, Schema schema) {

    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);
    TableEnvironmentInternal tableEnvInternal = (TableEnvironmentInternal) tableEnv;

    CatalogTable table = CatalogTable.of(schema, "comment", Arrays.asList(), properties);

    return FactoryUtil.createDynamicTableSource(
        null,
        ObjectIdentifier.of("", "", ""),
        tableEnvInternal.getCatalogManager().resolveCatalogTable(table),
        new HashMap<>(),
        new Configuration(),
        Thread.currentThread().getContextClassLoader(),
        false);
  }
}
