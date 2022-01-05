package com.github.knaufk.flink.faker;

import static org.apache.flink.configuration.ConfigOptions.key;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import net.datafaker.Faker;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;

public class FlinkFakerTableSourceFactory implements DynamicTableSourceFactory {

  public static final String IDENTIFIER = "faker";

  public static final String FIELDS = "fields";
  public static final String EXPRESSION = "expression";
  public static final String NULL_RATE = "null-rate";
  public static final String COLLECTION_LENGTH = "length";

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
          LogicalTypeRoot.ARRAY,
          LogicalTypeRoot.MAP,
          LogicalTypeRoot.ROW,
          LogicalTypeRoot.MULTISET,
          LogicalTypeRoot.DATE,
          LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE,
          LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
          LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
          LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE);

  public static final List<LogicalTypeRoot> COLLECTION_ROOT_TYPES =
      Arrays.asList(LogicalTypeRoot.ARRAY, LogicalTypeRoot.MAP, LogicalTypeRoot.MULTISET);

  @Override
  public FlinkFakerTableSource createDynamicTableSource(final Context context) {

    Configuration options = new Configuration();
    context.getCatalogTable().getOptions().forEach(options::setString);

    ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
    List<Column> physicalColumns =
        schema.getColumns().stream().filter(Column::isPhysical).collect(Collectors.toList());
    FieldInfo[] fieldInfos = new FieldInfo[physicalColumns.size()];

    for (int i = 0; i < fieldInfos.length; i++) {
      String fieldName = physicalColumns.get(i).getName();
      DataType dataType = physicalColumns.get(i).getDataType();
      validateDataType(fieldName, dataType);

      fieldInfos[i] = readAndValidateFieldInfo(options, fieldName, dataType.getLogicalType());
    }

    return new FlinkFakerTableSource(
        fieldInfos, schema, options.get(ROWS_PER_SECOND), options.get(NUMBER_OF_ROWS));
  }

  private Integer readAndValidateCollectionLength(
      Configuration options, String fieldName, LogicalType logicalType) {
    ConfigOption<Integer> collectionLength =
        key(FIELDS + "." + fieldName + "." + COLLECTION_LENGTH).intType().defaultValue(1);
    Integer fieldCollectionLength = options.get(collectionLength);

    if (fieldCollectionLength != 1 && !COLLECTION_ROOT_TYPES.contains(logicalType.getTypeRoot())) {
      throw new ValidationException(
          "Collection length may not be set for  "
              + fieldName
              + " with type "
              + logicalType.getTypeRoot().toString());
    }

    if (fieldCollectionLength <= 0 && COLLECTION_ROOT_TYPES.contains(logicalType.getTypeRoot())) {
      throw new ValidationException(
          "Collection length needs to be positive. Collection length of "
              + fieldName
              + " is "
              + fieldCollectionLength);
    }
    return fieldCollectionLength;
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

  private FieldInfo readAndValidateFieldInfo(
      Configuration options, String fieldName, LogicalType logicalType) {
    String[] fieldExpression;
    Map<String, FieldInfo> nestedFields = new HashMap<>();
    if (logicalType.getTypeRoot() == LogicalTypeRoot.MAP) {
      // expression is given with key and value
      ConfigOption<String> keyExpression =
          key(FIELDS + "." + fieldName + ".key." + EXPRESSION).stringType().noDefaultValue();
      ConfigOption<String> valueExpression =
          key(FIELDS + "." + fieldName + ".value." + EXPRESSION).stringType().noDefaultValue();
      fieldExpression = new String[] {options.get(keyExpression), options.get(valueExpression)};
      nestedFields.put(
          "key",
          readAndValidateFieldInfo(
              options, fieldName + ".key", ((MapType) logicalType).getKeyType()));
      nestedFields.put(
          "value",
          readAndValidateFieldInfo(
              options, fieldName + ".value", ((MapType) logicalType).getValueType()));
    } else if (logicalType.getTypeRoot() == LogicalTypeRoot.ARRAY) {
      String elementKey = FIELDS + "." + fieldName + ".element." + EXPRESSION;
      ConfigOption<String> elementExpression = key(elementKey).stringType().noDefaultValue();
      if (!options.contains(elementExpression)) {
        // For backward compatibility
        // as in previous versions there is no element configuration for arrays
        elementExpression =
            key(FIELDS + "." + fieldName + "." + EXPRESSION).stringType().noDefaultValue();
        String value = options.getValue(elementExpression);
        if (value != null) {
          options.setString(
              key(elementKey).stringType().noDefaultValue(), options.getValue(elementExpression));
        }
      }
      fieldExpression = new String[] {options.get(elementExpression)};
      nestedFields.put(
          "element",
          readAndValidateFieldInfo(
              options, fieldName + ".element", ((ArrayType) logicalType).getElementType()));
    } else if (logicalType.getTypeRoot() == LogicalTypeRoot.MULTISET) {
      String elementKey = FIELDS + "." + fieldName + ".element." + EXPRESSION;
      ConfigOption<String> elementExpression = key(elementKey).stringType().noDefaultValue();
      if (!options.contains(elementExpression)) {
        // For backward compatibility
        // as in previous versions there is no element configuration for arrays
        elementExpression =
            key(FIELDS + "." + fieldName + "." + EXPRESSION).stringType().noDefaultValue();
        String value = options.getValue(elementExpression);
        if (value != null) {
          options.setString(
              key(elementKey).stringType().noDefaultValue(), options.getValue(elementExpression));
        }
      }
      fieldExpression = new String[] {options.get(elementExpression)};
      nestedFields.put(
          "element",
          readAndValidateFieldInfo(
              options, fieldName + ".element", ((MultisetType) logicalType).getElementType()));
    } else if (logicalType.getTypeRoot() == LogicalTypeRoot.ROW) {
      List<RowType.RowField> rowFields = ((RowType) logicalType).getFields();
      fieldExpression = new String[rowFields.size()];
      // expression is given element by element
      for (int i = 0; i < rowFields.size(); i++) {
        ConfigOption<String> rowExpression =
            key(FIELDS + "." + fieldName + "." + rowFields.get(i).getName() + "." + EXPRESSION)
                .stringType()
                .noDefaultValue();
        fieldExpression[i] = options.get(rowExpression);
        nestedFields.put(
            rowFields.get(i).getName() + "_" + i,
            readAndValidateFieldInfo(
                options, fieldName + "." + rowFields.get(i).getName(), rowFields.get(i).getType()));
      }

    } else {
      fieldExpression =
          new String[] {
            options.get(
                key(FIELDS + "." + fieldName + "." + EXPRESSION).stringType().noDefaultValue())
          };
    }

    if (nestedFields.isEmpty()) {
      if (Arrays.asList(fieldExpression).contains(null)) {
        throw new ValidationException(
            "Every column needs a corresponding expression. No expression found for "
                + fieldName
                + ".");
      }

      try {
        Faker faker = new Faker();
        for (String expression : fieldExpression) faker.expression(expression);
      } catch (RuntimeException e) {
        throw new ValidationException("Invalid expression for column \"" + fieldName + "\".", e);
      }
    }
    return new FieldInfo(
        readAndValidateNullRate(options, fieldName),
        logicalType,
        fieldExpression,
        readAndValidateCollectionLength(options, fieldName, logicalType),
        nestedFields);
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
