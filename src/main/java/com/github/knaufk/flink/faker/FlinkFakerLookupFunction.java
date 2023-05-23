package com.github.knaufk.flink.faker;

import java.io.IOException;
import java.util.*;
import net.datafaker.Faker;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.FunctionRequirement;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.logical.LogicalType;

public class FlinkFakerLookupFunction extends LookupFunction {

  private String[][] fieldExpressions;
  private Locale[][] locales;
  private Float[] fieldNullRates;
  private Integer[] fieldCollectionLengths;
  private LogicalType[] types;
  private List<Integer> keyIndeces;
  private Faker faker;

  public FlinkFakerLookupFunction(
      String[][] fieldExpressions,
      Locale[][] locales,
      Float[] fieldNullRates,
      Integer[] fieldCollectionLengths,
      LogicalType[] types,
      int[][] keys) {
    this.fieldExpressions = fieldExpressions;
    this.locales = locales;
    this.fieldNullRates = fieldNullRates;
    this.fieldCollectionLengths = fieldCollectionLengths;
    this.types = types;

    keyIndeces = new ArrayList<>();
    for (int i = 0; i < keys.length; i++) {
      // we don't support nested rows for now, so this is actually one-dimensional
      keyIndeces.add(keys[i][0]);
    }
  }

  @Override
  public void open(FunctionContext context) throws Exception {
    super.open(context);
    faker = new Faker();
  }

  @Override
  public Collection<RowData> lookup(RowData keyRow) throws IOException {
    GenericRowData row = new GenericRowData(fieldExpressions.length);
    int keyCount = 0;
    for (int i = 0; i < fieldExpressions.length; i++) {
      if (keyIndeces.contains(i)) {
        row.setField(i, ((GenericRowData) keyRow).getField(keyCount));
        keyCount++;
      } else {
        float fieldNullRate = fieldNullRates[i];
        if (faker.random().nextFloat() > fieldNullRate) {
          List<String> values = new ArrayList<>();
          for (int j = 0; j < fieldCollectionLengths[i]; j++) {
            for (int k = 0; k < fieldExpressions[i].length; k++) {
              // loop for multiple expressions of one field (like map, row fields)
              final int finalI = i;
              final int finalK = k;
              values.add(
                  faker.doWith(
                      () -> faker.expression(fieldExpressions[finalI][finalK]), locales[i][k]));
            }
          }
          row.setField(
              i, FakerUtils.stringValueToType(values.toArray(new String[values.size()]), types[i]));
        } else {
          row.setField(i, null);
        }
      }
    }
    return Collections.singleton(row);
  }

  @Override
  public Set<FunctionRequirement> getRequirements() {
    return Collections.emptySet();
  }

  @Override
  public boolean isDeterministic() {
    return false;
  }
}
