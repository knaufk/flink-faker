package com.github.knaufk.flink.faker;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.*;

public class FlinkFakerTableSource implements ScanTableSource, LookupTableSource {

  private String[] fieldExpressions;
  private TableSchema schema;

  public FlinkFakerTableSource(String[] fieldExpressions, TableSchema schema) {
    this.fieldExpressions = fieldExpressions;
    this.schema = schema;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(final ScanContext scanContext) {
    return SourceFunctionProvider.of(new FlinkFakerSourceFunction(fieldExpressions, schema), false);
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
        new FlinkFakerLookupFunction(fieldExpressions, schema, context.getKeys()));
  }
}
