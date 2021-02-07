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
  public static final String NULL_RATE = "null-rate";

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
    Float[] fieldNullRates = new Float[schema.getFieldCount()];
    String[] fieldExpressions = new String[schema.getFieldCount()];

    for (int i = 0; i < fieldExpressions.length; i++) {
      String fieldName = schema.getFieldName(i).get();
      DataType dataType = schema.getFieldDataType(i).get();
      validateDataType(fieldName, dataType);

      fieldExpressions[i] = readAndValidateFieldExpression(options, fieldName);
      ;
      fieldNullRates[i] = readAndValidateNullRate(options, fieldName);
      ;
    }
    return new FlinkFakerTableSource(
        fieldExpressions,
        fieldNullRates,
        schema,
        options.get(ROWS_PER_SECOND),
        options.get(NUMBER_OF_ROWS));
  }

  private Float readAndValidateNullRate(Configuration options, String fieldName) {
    ConfigOption<Float> nullRate =
        key(FIELDS + "." + fieldName + "." + NULL_RATE).floatType().defaultValue(0.0f);

    Float fieldNullRate = options.get(nullRate);

    if (fieldNullRate > 1.0f || fieldNullRate < 0.0f) {
      throw new ValidationException(
          "Null rate needs to be in [0,1]. Null rate of " + fieldName + " is " + fieldNullRate);
    }
    return fieldNullRate;
  }

  private String readAndValidateFieldExpression(Configuration options, String fieldName) {
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
      Faker faker = new Faker();
      faker.expression(fieldExpression);
    } catch (RuntimeException e) {
      throw new ValidationException("Invalid expression for column \"" + fieldName + "\".", e);
    }
    return fieldExpression;
  }

  private void validateDataType(String fieldName, DataType dataType) {
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
