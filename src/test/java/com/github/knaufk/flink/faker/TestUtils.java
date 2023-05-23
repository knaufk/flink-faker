package com.github.knaufk.flink.faker;

import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;

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
        {"#{date.future '2','1','DAYS'}"},
        {"#{time.future '2','1','SECONDS'}"},
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

  public static Locale[][] getDefaultLocales(int size) {
    Locale[][] locales = new Locale[size][];
    Arrays.fill(locales, new Locale[] {Locale.ENGLISH, Locale.ENGLISH});
    return locales;
  }
}
