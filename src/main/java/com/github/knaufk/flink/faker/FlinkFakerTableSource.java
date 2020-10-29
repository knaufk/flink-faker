package com.github.knaufk.flink.faker;

import java.util.Arrays;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.*;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

public class FlinkFakerTableSource implements ScanTableSource, LookupTableSource {

  private String[] fieldExpressions;
  private TableSchema schema;
  private final LogicalType[] types;

  public FlinkFakerTableSource(String[] fieldExpressions, TableSchema schema) {
    this.fieldExpressions = fieldExpressions;
    this.schema = schema;
    types =
        Arrays.stream(schema.getFieldDataTypes())
            .map(DataType::getLogicalType)
            .toArray(LogicalType[]::new);
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(final ScanContext scanContext) {
    return SourceFunctionProvider.of(new FlinkFakerSourceFunction(fieldExpressions, types), false);
  }

  @Override
  public DynamicTableSource copy() {
    return new FlinkFakerTableSource(fieldExpressions, schema);
  }

  @Override
  public String asSummaryString() {
    return "FlinkFakerSource";
  }

  @Override
  public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
    return TableFunctionProvider.of(
        new FlinkFakerLookupFunction(fieldExpressions, types, context.getKeys()));
  }
}
