package com.github.knaufk.flink.faker;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.table.types.logical.*;

public class TestUtils {

  public static final LogicalType[] ALL_SUPPORTED_DATA_TYPES =
      new LogicalType[] {
        new TinyIntType(),
        new SmallIntType(),
        new IntType(),
        new BigIntType(),
        new DoubleType(),
        new FloatType(),
        new DecimalType(6, 2),
        new CharType(10),
        new VarCharType(255),
        new VarCharType(Integer.MAX_VALUE),
        new BooleanType(),
        new TimestampType(),
        new DateType(),
        new TimeType(),
        new ArrayType(new IntType()),
        new MapType(new IntType(), new VarCharType()),
        new RowType(
            Arrays.asList(
                new RowType.RowField("age", new IntType()),
                new RowType.RowField("name", new CharType(10)))),
        new MultisetType(new CharType(10)),
        new ArrayType(new ArrayType(new IntType())),
        new ArrayType(new MapType(new IntType(), new VarCharType())),
        new MapType(
            new MultisetType(new IntType()),
            new RowType(
                Arrays.asList(
                    new RowType.RowField("age", new IntType()),
                    new RowType.RowField("name", new CharType(10))))),
      };
  public static final String[][] EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES =
      new String[][] {
        {"#{number.numberBetween '-128','127'}"},
        {"#{number.numberBetween '-32768','32767'}"},
        {"#{number.numberBetween '-2147483648','2147483647'}"},
        {"#{number.randomNumber '12','false'}"},
        {"#{number.randomDouble '3','-1000','1000'}"},
        {"#{number.randomDouble '3','-1000','1000'}"},
        {"#{number.randomDouble '3','-1000','1000'}"},
        {"#{Lorem.characters '10'}"},
        {"#{Lorem.characters '255'}"},
        {"#{Lorem.sentence}"},
        {"#{regexify '(true|false){1}'}"},
        {"#{date.past '15','5','SECONDS'}"},
        {"#{date.future '2','1','DAYS'}"},
        {"#{time.future '2','1','SECONDS'}"},
        {"#{number.numberBetween '-128','127'}"},
        {"#{number.numberBetween '-128','127'}", "#{Lorem.characters '10'}"},
        {"#{number.numberBetween '-128','127'}", "#{Lorem.characters '10'}"},
        {"#{Lorem.characters '10'}"},
        {"#{number.numberBetween '-128','127'}"},
        {"#{number.numberBetween '-128','127'}", "#{Lorem.characters '10'}"},
        {"#{number.numberBetween '-128','127'}", "#{number.numberBetween '-128','127'}"}
      };

  public static FieldInfo[] constructFieldInfos(
      String[][] expressions, LogicalType[] logicalTypes, Float[] nullRates, Integer[] length) {
    if (expressions.length != nullRates.length
        || nullRates.length != length.length
        || nullRates.length != logicalTypes.length) {
      throw new IllegalArgumentException("Length of all incoming arrays should be same");
    }
    FieldInfo[] fieldInfos = new FieldInfo[expressions.length];
    for (int i = 0; i < expressions.length; i++) {
      fieldInfos[i] = constructFieldInfo(expressions[i], logicalTypes[i], nullRates[i], length[i]);
    }
    return fieldInfos;
  }

  private static FieldInfo constructFieldInfo(
      String[] expressions, LogicalType logicalType, Float nullRate, Integer length) {
    Map<String, FieldInfo> nestedFields = new HashMap<>();
    switch (logicalType.getTypeRoot()) {
      case ARRAY:
        FieldInfo arrayElement =
            constructFieldInfo(expressions, ((ArrayType) logicalType).getElementType(), 0f, 1);
        nestedFields.put("element", arrayElement);
        return new FieldInfo(nullRate, logicalType, new String[0], length, nestedFields);
      case MULTISET:
        FieldInfo multisetElement =
            constructFieldInfo(expressions, ((MultisetType) logicalType).getElementType(), 0f, 1);
        nestedFields.put("element", multisetElement);
        return new FieldInfo(nullRate, logicalType, new String[0], length, nestedFields);
      case MAP:
        FieldInfo keyElement =
            constructFieldInfo(expressions, ((MapType) logicalType).getKeyType(), 0f, 1);
        FieldInfo valueElement =
            constructFieldInfo(expressions, ((MapType) logicalType).getValueType(), 0f, 1);
        nestedFields.put("key", keyElement);
        nestedFields.put("value", valueElement);
        return new FieldInfo(nullRate, logicalType, new String[0], length, nestedFields);
      case ROW:
        for (int j = 0; j < ((RowType) logicalType).getFieldCount(); j++) {
          FieldInfo fieldInfo =
              constructFieldInfo(expressions, ((RowType) logicalType).getTypeAt(j), 0f, 1);
          nestedFields.put(((RowType) logicalType).getFieldNames().get(j) + "_" + j, fieldInfo);
        }
        return new FieldInfo(nullRate, logicalType, new String[0], length, nestedFields);
      default:
        return new FieldInfo(nullRate, logicalType, expressions, length, null);
    }
  }

  public static Float[] neverNull(int size) {
    return getNullRates(size, 0.0f);
  }

  public static Float[] alwaysNull(int size) {
    return getNullRates(size, 1.0f);
  }

  private static Float[] getNullRates(int size, float v) {
    Float[] zeroNullRates = new Float[size];
    Arrays.fill(zeroNullRates, v);
    return zeroNullRates;
  }

  public static Integer[] getArrayOfOnes(int size) {
    Integer[] array = new Integer[size];
    Arrays.fill(array, 1);
    return array;
  }
}
