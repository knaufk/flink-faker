package com.github.knaufk.flink.faker;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.jupiter.api.Test;

class FlinkFakerTableSourceFactoryTest {

  private static final TableSchema VALID_SCHEMA =
      TableSchema.builder()
          .field("f0", DataTypes.STRING())
          .field("f1", DataTypes.VARCHAR(100))
          .field("f2", DataTypes.CHAR(2))
          .build();

  private static final TableSchema INVALID_SCHEMA =
      TableSchema.builder()
          .field("f0", DataTypes.STRING())
          .field("f1", DataTypes.VARCHAR(100))
          .field("f2", DataTypes.DOUBLE())
          .build();

  @Test
  public void testSchemaWithNonStringTypesIsInvalid() {

    assertThatExceptionOfType(ValidationException.class)
        .isThrownBy(
            () -> {
              DescriptorProperties descriptorProperties = new DescriptorProperties();
              descriptorProperties.putString(FactoryUtil.CONNECTOR.key(), "faker");
              descriptorProperties.putString("fields.f0.expression", "#{number.randomDigit}");
              descriptorProperties.putString("fields.f1.expression", "#{number.randomDigit}");
              descriptorProperties.putString("fields.f2.expression", "#{number.randomDigit}");

              createTableSource(descriptorProperties, INVALID_SCHEMA);
            })
        .withStackTraceContaining("f2 is DOUBLE.");
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
  public void testValidTableSourceIsValid() {

    DescriptorProperties descriptorProperties = new DescriptorProperties();
    descriptorProperties.putString(FactoryUtil.CONNECTOR.key(), "faker");
    descriptorProperties.putString("fields.f0.expression", "#{number.randomDigit}");
    descriptorProperties.putString("fields.f1.expression", "#{number.randomDigit}");
    descriptorProperties.putString("fields.f2.expression", "#{number.randomDigit}");

    createTableSource(descriptorProperties, VALID_SCHEMA);
  }

  private void createTableSource(
      DescriptorProperties descriptorProperties, final TableSchema invalidSchema) {
    DynamicTableSource tableSource =
        FactoryUtil.createTableSource(
            null,
            ObjectIdentifier.of("", "", ""),
            new CatalogTableImpl(invalidSchema, descriptorProperties.asMap(), ""),
            new Configuration(),
            Thread.currentThread().getContextClassLoader());
  }
}
