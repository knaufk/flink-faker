package com.github.knaufk.flink.faker;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.jupiter.api.Test;

class FlinkFakerTableSourceFactoryTest {

  private static final TableSchema VALID_SCHEMA =
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

  private static final TableSchema INVALID_SCHEMA =
      TableSchema.builder()
          .field("f0", DataTypes.STRING())
          .field("f1", DataTypes.VARCHAR(100))
          .field("f2", DataTypes.DATE())
          .build();

  private static final TableSchema TINY_SCHEMA =
      TableSchema.builder().field("f0", DataTypes.TINYINT()).build();

  @Test
  public void testSchemaWithNonSupportedTypesIsInvalid() {

    assertThatExceptionOfType(ValidationException.class)
        .isThrownBy(
            () -> {
              DescriptorProperties descriptorProperties = new DescriptorProperties();
              descriptorProperties.putString(FactoryUtil.CONNECTOR.key(), "faker");
              descriptorProperties.putString(
                  "fields.f0.expression", "#{number.numberBetween '-128','127'}");
              descriptorProperties.putString(
                  "fields.f1.expression", "#{number.numberBetween '-32768','32767'}");
              descriptorProperties.putString(
                  "fields.f2.expression", "#{number.numberBetween '-2147483648','2147483647'}");
              descriptorProperties.putString(
                  "fields.f3.expression", "#{number.randomNumber '12','false'}");
              descriptorProperties.putString(
                  "fields.f4.expression", "#{number.randomDouble '3','-1000','1000'}");
              descriptorProperties.putString(
                  "fields.f5.expression", "#{number.randomDouble '3','-1000','1000'}");
              descriptorProperties.putString(
                  "fields.f6.expression", "#{number.randomDouble '3','-1000','1000'}");
              descriptorProperties.putString("fields.f7.expression", "#{Lorem.characters '10'}");
              descriptorProperties.putString("fields.f8.expression", "#{Lorem.characters '255'}");
              descriptorProperties.putString("fields.f9.expression", "#{Lorem.sentence}");
              descriptorProperties.putString(
                  "fields.f10.expression", "#{regexify '(true|false){1}'}");
              createTableSource(descriptorProperties, INVALID_SCHEMA);
            })
        .withStackTraceContaining("f2 is DATE.");
  }

  @Test
  public void testValidNullRateIsValid() {
    DescriptorProperties descriptorProperties = new DescriptorProperties();
    descriptorProperties.putString(FactoryUtil.CONNECTOR.key(), "faker");
    descriptorProperties.putString("fields.f0.expression", "#{number.numberBetween '-128','127'}");
    descriptorProperties.putString("fields.f0.null-rate", "0.1");

    createTableSource(descriptorProperties, TINY_SCHEMA);
  }

  @Test
  public void testNegativeNullRateIsInvalid() {

    assertThatExceptionOfType(ValidationException.class)
        .isThrownBy(
            () -> {
              DescriptorProperties descriptorProperties = new DescriptorProperties();
              descriptorProperties.putString(FactoryUtil.CONNECTOR.key(), "faker");
              descriptorProperties.putString(
                  "fields.f0.expression", "#{number.numberBetween '-128','127'}");
              descriptorProperties.putString("fields.f0.null-rate", "-0.8");

              createTableSource(descriptorProperties, TINY_SCHEMA);
            })
        .withStackTraceContaining("needs to be in [0,1]");
  }

  @Test
  public void testNullRateGreaterOneIsInvalid() {

    assertThatExceptionOfType(ValidationException.class)
        .isThrownBy(
            () -> {
              DescriptorProperties descriptorProperties = new DescriptorProperties();
              descriptorProperties.putString(FactoryUtil.CONNECTOR.key(), "faker");
              descriptorProperties.putString(
                  "fields.f0.expression", "#{number.numberBetween '-128','127'}");
              descriptorProperties.putString("fields.f0.null-rate", "1.01");

              createTableSource(descriptorProperties, TINY_SCHEMA);
            })
        .withStackTraceContaining("needs to be in [0,1]");
  }

  @Test
  public void testPropertiesWithoutExpressionForOneFieldIsInvalid() {

    assertThatExceptionOfType(ValidationException.class)
        .isThrownBy(
            () -> {
              DescriptorProperties descriptorProperties = new DescriptorProperties();
              descriptorProperties.putString(FactoryUtil.CONNECTOR.key(), "faker");
              descriptorProperties.putString("fields.f0.expression", "#{number.randomDigit}");
              descriptorProperties.putString("fields.f1.expression", "#{number.randomDigit}");

              createTableSource(descriptorProperties, VALID_SCHEMA);
            })
        .withStackTraceContaining("No expression found for f2.");
  }

  @Test
  public void testInvalidExpressionIsInvalid() {

    assertThatExceptionOfType(ValidationException.class)
        .isThrownBy(
            () -> {
              DescriptorProperties descriptorProperties = new DescriptorProperties();
              descriptorProperties.putString(FactoryUtil.CONNECTOR.key(), "faker");
              descriptorProperties.putString("fields.f0.expression", "#{number.abc}");
              descriptorProperties.putString("fields.f1.expression", "#{number.randomDigit}");

              createTableSource(descriptorProperties, VALID_SCHEMA);
            })
        .withStackTraceContaining("Invalid expression for column \"f0\".");
  }

  @Test
  public void testValidTableSourceIsValid() {

    DescriptorProperties descriptorProperties = new DescriptorProperties();
    descriptorProperties.putString(FactoryUtil.CONNECTOR.key(), "faker");
    descriptorProperties.putString("fields.f0.expression", "#{number.numberBetween '-128','127'}");
    descriptorProperties.putString(
        "fields.f1.expression", "#{number.numberBetween '-32768','32767'}");
    descriptorProperties.putString(
        "fields.f2.expression", "#{number.numberBetween '-2147483648','2147483647'}");
    descriptorProperties.putString("fields.f3.expression", "#{number.randomNumber '12','false'}");
    descriptorProperties.putString(
        "fields.f4.expression", "#{number.randomDouble '3','-1000','1000'}");
    descriptorProperties.putString(
        "fields.f5.expression", "#{number.randomDouble '3','-1000','1000'}");
    descriptorProperties.putString(
        "fields.f6.expression", "#{number.randomDouble '3','-1000','1000'}");
    descriptorProperties.putString("fields.f7.expression", "#{Lorem.characters '10'}");
    descriptorProperties.putString("fields.f8.expression", "#{Lorem.characters '255'}");
    descriptorProperties.putString("fields.f9.expression", "#{Lorem.sentence}");
    descriptorProperties.putString("fields.f10.expression", "#{regexify '(true|false){1}'}");

    createTableSource(descriptorProperties, VALID_SCHEMA);
  }

  @Test
  public void testTimestamps() {

    DescriptorProperties descriptorProperties = new DescriptorProperties();
    descriptorProperties.putString(FactoryUtil.CONNECTOR.key(), "faker");

    descriptorProperties.putString("fields.f0.expression", "#{date.past '15','SECONDS'}");
    descriptorProperties.putString("fields.f1.expression", "#{date.past '15','SECONDS'}");
    descriptorProperties.putString("fields.f2.expression", "#{date.past '15','SECONDS'}");

    TableSchema tableSchema =
        TableSchema.builder()
            .field("f0", DataTypes.TIMESTAMP())
            .field("f1", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE())
            .field("f2", DataTypes.TIMESTAMP_WITH_TIME_ZONE())
            .build();

    createTableSource(descriptorProperties, tableSchema);
  }

  private DynamicTableSource createTableSource(
      DescriptorProperties descriptorProperties, final TableSchema invalidSchema) {

    EnvironmentSettings settings =
    EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);
    TableEnvironmentInternal tableEnvInternal = (TableEnvironmentInternal) tableEnv;
    CatalogTable catalogTable = new CatalogTableImpl(invalidSchema, descriptorProperties.asMap(), "");

    return FactoryUtil.createTableSource(
        null,
        ObjectIdentifier.of("", "", ""),
        tableEnvInternal.getCatalogManager().resolveCatalogTable(catalogTable),
        new Configuration(),
        Thread.currentThread().getContextClassLoader(),
        false);
  }
}
