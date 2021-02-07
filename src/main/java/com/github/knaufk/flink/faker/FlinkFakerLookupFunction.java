package com.github.knaufk.flink.faker;

import com.github.javafaker.Faker;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

public class FlinkFakerLookupFunction extends TableFunction<RowData> {

  private String[] fieldExpressions;
  private Float[] fieldNullRates;
  private LogicalType[] types;
  private int[][] keys;
  private List<Integer> keyIndeces;
  private Faker faker;
  private Random rand;

  public FlinkFakerLookupFunction(
      String[] fieldExpressions, Float[] fieldNullRates, LogicalType[] types, int[][] keys) {
    this.fieldExpressions = fieldExpressions;
    this.fieldNullRates = fieldNullRates;
    this.types = types;

    keyIndeces = new ArrayList<>();
    for (int i = 0; i < keys.length; i++) {
      // we don't support nested rows for now, so this is actually one-dimensional
      keyIndeces.add(keys[i][0]);
    }

    this.keys = keys;
  }

  @Override
  public void open(FunctionContext context) throws Exception {
    super.open(context);
    faker = new Faker();
    rand = new Random();
  }

  public void eval(Object... keys) {
    GenericRowData row = new GenericRowData(fieldExpressions.length);
    int keyCount = 0;
    for (int i = 0; i < fieldExpressions.length; i++) {
      if (keyIndeces.contains(i)) {
        row.setField(i, keys[keyCount]);
        keyCount++;
      } else {
        float fieldNullRate = fieldNullRates[i];
        LogicalTypeRoot typeRoot = (types[i]).getTypeRoot();
        if (rand.nextFloat() > fieldNullRate) {
          String value = faker.expression(fieldExpressions[i]);
          row.setField(i, FakerUtils.stringValueToType(value, typeRoot));
        } else {
          row.setField(i, null);
        }
      }
    }
    collect(row);
  }
}
