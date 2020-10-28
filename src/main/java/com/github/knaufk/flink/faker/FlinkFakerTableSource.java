package com.github.knaufk.flink.faker;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;

public class FlinkFakerTableSource implements ScanTableSource {

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
}
