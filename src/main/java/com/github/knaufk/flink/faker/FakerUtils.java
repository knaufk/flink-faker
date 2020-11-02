package com.github.knaufk.flink.faker;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

public class FakerUtils {

  public static final String FAKER_DATETIME_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";

  private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern(FAKER_DATETIME_FORMAT);;

  static Object stringValueToType(String value, LogicalTypeRoot logicalType) {

    switch (logicalType) {
      case CHAR:
        return StringData.fromString(value);
      case VARCHAR:
        return StringData.fromString(value);
      case BOOLEAN:
        return Boolean.parseBoolean(value);
      case DECIMAL:
        return new BigDecimal(value);
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
        //      case DATE:
        //        break;
      case TIME_WITHOUT_TIME_ZONE:
        return TimestampData.fromInstant(Instant.from(formatter.parse(value)));
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return TimestampData.fromInstant(Instant.from(formatter.parse(value)));
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return TimestampData.fromInstant(Instant.from(formatter.parse(value)));
        //        break;
        //              case INTERVAL_YEAR_MONTH:
        //        break;
        //      case INTERVAL_DAY_TIME:
        //        break;
        //      case ARRAY:
        //        break;
        //      case MULTISET:
        //        break;
        //      case MAP:
        //        break;
        //      case ROW:
        //        break;
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
