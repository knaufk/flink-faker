package com.github.knaufk.flink.faker;

import static org.apache.flink.configuration.ConfigOptions.key;

import java.util.HashSet;
import java.util.Set;

import com.github.javafaker.Faker;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;

public class FlinkFakerTableSourceFactory implements DynamicTableSourceFactory {

  public static final String IDENTIFIER = "faker";

  public static final String FIELDS = "fields";
  public static final String EXPRESSION = "expression";

  @Override
  public DynamicTableSource createDynamicTableSource(final Context context) {

    Faker faker = new Faker();

    CatalogTable catalogTable = context.getCatalogTable();

    Configuration options = new Configuration();
    context.getCatalogTable().getOptions().forEach(options::setString);

    TableSchema schema = TableSchemaUtils.getPhysicalSchema(catalogTable.getSchema());
    String[] fieldExpressions = new String[schema.getFieldCount()];

    for (int i = 0; i < fieldExpressions.length; i++) {
      DataType dataType = schema.getFieldDataType(i).get();
      String fieldName = schema.getFieldName(i).get();
      if (!dataType.getConversionClass().isAssignableFrom(String.class)) {
        throw new ValidationException(
            "Only CHAR/VARCHAR/STRING columns are supported by Faker TableSource. "
                + fieldName
                + " is "
                + dataType.getLogicalType()
                + ".");
      }

      ConfigOption<String> expression =
          key(FIELDS + "." + fieldName + "." + EXPRESSION).stringType().noDefaultValue();

      String fieldExpression = options.get(expression);
      if (fieldExpression == null) {
        throw new ValidationException(
            "Every column needs a corresponding expression. No expression found for "
                + fieldName
                + ".");
      }

      try {
        faker.expression(fieldExpression);
      } catch (RuntimeException e) {
        throw new ValidationException("Invalid expression for column \"" + fieldName + "\".", e);
      }

      fieldExpressions[i] = fieldExpression;
    }
    return new FlinkFakerTableSource(fieldExpressions);
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return new HashSet<>();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return new HashSet<>();
  }
}
