package com.github.knaufk.flink.faker;

import java.util.Arrays;
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
        new ArrayType(new IntType()),
        new MapType(new IntType(), new VarCharType()),
        new RowType(
            Arrays.asList(
                new RowType.RowField("age", new IntType()),
                new RowType.RowField("name", new CharType(10)))),
        new MultisetType(new CharType(10))
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
        {"#{number.numberBetween '-128','127'}"},
        {"#{number.numberBetween '-128','127'}", "#{Lorem.characters '10'}"},
        {"#{number.numberBetween '-128','127'}", "#{Lorem.characters '10'}"},
        {"#{Lorem.characters '10'}"}
      };

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
