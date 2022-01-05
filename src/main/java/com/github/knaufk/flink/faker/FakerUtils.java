package com.github.knaufk.flink.faker;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

public class FakerUtils {

  private static final DateTimeFormatter FORMATTER =
      new DateTimeFormatterBuilder()
          // Pattern was taken from java.sql.Timestamp#toString
          .appendPattern("uuuu-MM-dd HH:mm:ss")
          .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
          .toFormatter(Locale.US);

  static Object stringValueToType(
      FieldInfo fieldInfo, Function<String, String> fakerExpression2Value) {
    if (fieldInfo.getNullRate() == 1.0
        || fieldInfo.getNullRate() > 0
            && fieldInfo.getNullRate() > ThreadLocalRandom.current().nextFloat()) {
      return null;
    }
    String[] fakerExpressions = fieldInfo.getExpressions();
    String value =
        fieldInfo.getNestedFields() == null || fieldInfo.getNestedFields().isEmpty()
            ? fakerExpression2Value.apply(fakerExpressions[0])
            : "";
    LogicalType logicalType = fieldInfo.getLogicalType();
    switch (logicalType.getTypeRoot()) {
      case CHAR:
      case VARCHAR:
        return StringData.fromString(value);
      case BOOLEAN:
        return Boolean.parseBoolean(value);
      case DECIMAL:
        BigDecimal bd = new BigDecimal(value);
        return DecimalData.fromBigDecimal(bd, bd.precision(), bd.scale());
      case TINYINT:
        return Byte.parseByte(value);
      case SMALLINT:
        return Short.parseShort(value);
      case INTEGER:
        return Integer.parseInt(value);
      case BIGINT:
        return new BigInteger(value);
      case FLOAT:
        return Float.parseFloat(value);
      case DOUBLE:
        return Double.parseDouble(value);
      case DATE:
        return (int)
            (Date.from(Instant.from(FORMATTER.withZone(ZoneId.systemDefault()).parse(value)))
                    .getTime()
                / (86400 * 1000));
      case TIME_WITHOUT_TIME_ZONE:
        return (int) (Long.parseLong(value) / 1000_000L);
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return TimestampData.fromInstant(
            Instant.from(FORMATTER.withZone(ZoneId.systemDefault()).parse(value)));
        //        break;
        //              case INTERVAL_YEAR_MONTH:
        //        break;
        //      case INTERVAL_DAY_TIME:
        //        break;
      case ARRAY:
        Object[] arrayElements = new Object[fieldInfo.getLength()];
        for (int i = 0; i < fieldInfo.getLength(); i++)
          arrayElements[i] =
              (stringValueToType(
                  fieldInfo.getNestedFields().get("element"), fakerExpression2Value));
        return new GenericArrayData(arrayElements);
      case MULTISET:
        Map<Object, Integer> multisetMap = new HashMap<>();
        for (int i = 0; i < fieldInfo.getLength(); i++) {
          Object element =
              stringValueToType(fieldInfo.getNestedFields().get("element"), fakerExpression2Value);
          Integer multiplicity =
              multisetMap.containsKey(element) ? (multisetMap.get(element) + 1) : 1;
          multisetMap.put(element, multiplicity);
        }
        return new GenericMapData(multisetMap);
      case MAP:
        Map<Object, Object> map = new HashMap<>();
        for (int i = 0; i < fieldInfo.getLength(); i++) {
          Object key =
              stringValueToType(fieldInfo.getNestedFields().get("key"), fakerExpression2Value);
          Object val =
              stringValueToType(fieldInfo.getNestedFields().get("value"), fakerExpression2Value);
          map.put(key, val);
        }
        return new GenericMapData(map);
      case ROW:
        GenericRowData row = new GenericRowData(((RowType) logicalType).getFieldCount());
        for (int i = 0; i < ((RowType) logicalType).getFieldCount(); i++) {
          Object obj =
              stringValueToType(
                  fieldInfo
                      .getNestedFields()
                      .get(((RowType) logicalType).getFieldNames().get(i) + "_" + i),
                  fakerExpression2Value);
          row.setField(i, obj);
        }
        return row;
        //      case DISTINCT_TYPE:
        //        break;
        //      case STRUCTURED_TYPE:
        //        break;
        //      case NULL:
        //        break;
        //      case RAW:
        //        break;
        //      case SYMBOL:
        //        break;
        //      case UNRESOLVED:
        //        break;
      default:
        throw new RuntimeException("Unsupported Data Type");
    }
  }
}
