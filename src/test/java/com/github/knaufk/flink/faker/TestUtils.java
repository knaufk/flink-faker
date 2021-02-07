package com.github.knaufk.flink.faker;

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
        new TimestampType()
      };
  public static final String[] EXPRESSIONS_FOR_ALL_SUPPORTED_DATATYPES =
      new String[] {
        "#{number.numberBetween '-128','127'}",
        "#{number.numberBetween '-32768','32767'}",
        "#{number.numberBetween '-2147483648','2147483647'}",
        "#{number.randomNumber '12','false'}",
        "#{number.randomDouble '3','-1000','1000'}",
        "#{number.randomDouble '3','-1000','1000'}",
        "#{number.randomDouble '3','-1000','1000'}",
        "#{Lorem.characters '10'}",
        "#{Lorem.characters '255'}",
        "#{Lorem.sentence}",
        "#{regexify '(true|false){1}'}",
        "#{date.past '15','5','SECONDS'}"
      };

  public static Float[] neverNull(int size) {
    return getNullRates(size, 0.0f);
  }

  public static Float[] alwaysNull(int size) {
    return getNullRates(size, 1.0f);
  }

  private static Float[] getNullRates(int size, float v) {
    Float[] zeroNullRates = new Float[size];
    for (int i = 0; i < zeroNullRates.length; i++) {
      zeroNullRates[i] = v;
    }
    return zeroNullRates;
  }
}
