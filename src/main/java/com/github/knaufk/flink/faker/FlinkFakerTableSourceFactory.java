package com.github.knaufk.flink.faker;

import static org.apache.flink.configuration.ConfigOptions.key;

import com.github.javafaker.Faker;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.utils.TableSchemaUtils;

public class FlinkFakerTableSourceFactory implements DynamicTableSourceFactory {

  public static final String IDENTIFIER = "faker";

  public static final String FIELDS = "fields";
  public static final String EXPRESSION = "expression";

  public static final Long ROWS_PER_SECOND_DEFAULT_VALUE = 10000L;
  public static final Long UNLIMITED_ROWS = -1L;

  public static final ConfigOption<Long> ROWS_PER_SECOND =
      key("rows-per-second")
          .longType()
          .defaultValue(ROWS_PER_SECOND_DEFAULT_VALUE)
          .withDescription("Rows per second to control the emit rate.");

  public static final ConfigOption<Long> NUMBER_OF_ROWS =
      key("number-of-rows")
          .longType()
          .defaultValue(UNLIMITED_ROWS)
          .withDescription("Total number of rows to emit. By default, the source is unbounded.");

  public static final List<LogicalTypeRoot> SUPPORTED_ROOT_TYPES =
      Arrays.asList(
          LogicalTypeRoot.DOUBLE,
          LogicalTypeRoot.FLOAT,
          LogicalTypeRoot.DECIMAL,
          LogicalTypeRoot.TINYINT,
          LogicalTypeRoot.SMALLINT,
          LogicalTypeRoot.INTEGER,
          LogicalTypeRoot.BIGINT,
          LogicalTypeRoot.CHAR,
          LogicalTypeRoot.VARCHAR,
          LogicalTypeRoot.BOOLEAN,
          LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
          LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
          LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE);

  @Override
  public FlinkFakerTableSource createDynamicTableSource(final Context context) {

    Faker faker = new Faker();

    CatalogTable catalogTable = context.getCatalogTable();

    Configuration options = new Configuration();
    context.getCatalogTable().getOptions().forEach(options::setString);

    TableSchema schema = TableSchemaUtils.getPhysicalSchema(catalogTable.getSchema());
    String[] fieldExpressions = new String[schema.getFieldCount()];

    for (int i = 0; i < fieldExpressions.length; i++) {
      DataType dataType = schema.getFieldDataType(i).get();
      String fieldName = schema.getFieldName(i).get();
      if (!SUPPORTED_ROOT_TYPES.contains(dataType.getLogicalType().getTypeRoot())) {
        throw new ValidationException(
            "Only "
                + SUPPORTED_ROOT_TYPES
                + " columns are supported by Faker TableSource. "
                + fieldName
                + " is "
                + dataType.getLogicalType().getTypeRoot()
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
    return new FlinkFakerTableSource(
        fieldExpressions, schema, options.get(ROWS_PER_SECOND), options.get(NUMBER_OF_ROWS));
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
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(ROWS_PER_SECOND);
    options.add(NUMBER_OF_ROWS);
    return options;
  }
}
