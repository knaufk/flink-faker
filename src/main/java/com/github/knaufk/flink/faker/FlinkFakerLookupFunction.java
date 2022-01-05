package com.github.knaufk.flink.faker;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import net.datafaker.Faker;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;

public class FlinkFakerLookupFunction extends TableFunction<RowData> {

  private final FieldInfo[] fieldInfos;
  private List<Integer> keyIndeces;
  private Faker faker;
  private Random rand;

  public FlinkFakerLookupFunction(FieldInfo[] fieldInfos, int[][] keys) {
    this.fieldInfos = fieldInfos;

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
    rand = new Random();
  }

  public void eval(Object... keys) {
    GenericRowData row = new GenericRowData(fieldInfos.length);
    int keyCount = 0;
    for (int i = 0; i < fieldInfos.length; i++) {
      if (keyIndeces.contains(i)) {
        row.setField(i, keys[keyCount]);
        keyCount++;
      } else {
        row.setField(i, FakerUtils.stringValueToType(fieldInfos[i], f -> faker.expression(f)));
      }
    }
    collect(row);
  }
}
