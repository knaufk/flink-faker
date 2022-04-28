package com.github.knaufk.flink.faker;

import static com.github.knaufk.flink.faker.FlinkFakerTableSourceFactory.UNLIMITED_ROWS;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.*;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

public class FlinkFakerTableSource
    implements ScanTableSource, LookupTableSource, SupportsLimitPushDown {

  private String[][] fieldExpressions;
  private Float[] fieldNullRates;
  private Integer[] fieldCollectionLengths;
  private ResolvedSchema schema;
  private final LogicalType[] types;
  private long rowsPerSecond;
  private long numberOfRows;

  public FlinkFakerTableSource(
      String[][] fieldExpressions,
      Float[] fieldNullRates,
      Integer[] fieldCollectionLengths,
      ResolvedSchema schema,
      long rowsPerSecond,
      long numberOfRows) {
    this.fieldExpressions = fieldExpressions;
    this.fieldNullRates = fieldNullRates;
    this.fieldCollectionLengths = fieldCollectionLengths;
    this.schema = schema;
    types =
        schema.getColumns().stream()
            .filter(column -> column.isPhysical())
            .map(Column::getDataType)
            .map(DataType::getLogicalType)
            .toArray(LogicalType[]::new);
    this.rowsPerSecond = rowsPerSecond;
    this.numberOfRows = numberOfRows;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(final ScanContext scanContext) {
    boolean isBounded = numberOfRows != UNLIMITED_ROWS;

    return new DataStreamScanProvider() {
      @Override
      public DataStream<RowData> produceDataStream(
          ProviderContext providerContext, StreamExecutionEnvironment env) {

        long to = isBounded ? numberOfRows : Long.MAX_VALUE;
        DataStreamSource<Long> sequence =
            env.fromSource(
                new NumberSequenceSource(1, to),
                WatermarkStrategy.noWatermarks(),
                "Source Generator");

        return sequence.flatMap(
            new FlinkFakerGenerator(
                fieldExpressions, fieldNullRates, fieldCollectionLengths, types, rowsPerSecond));
      }

      @Override
      public boolean isBounded() {
        return isBounded;
      }
    };
  }

  @Override
  public DynamicTableSource copy() {
    return new FlinkFakerTableSource(
        fieldExpressions,
        fieldNullRates,
        fieldCollectionLengths,
        schema,
        rowsPerSecond,
        numberOfRows);
  }

  @Override
  public String asSummaryString() {
    return "FlinkFakerSource";
  }

  @Override
  public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
    return TableFunctionProvider.of(
        new FlinkFakerLookupFunction(
            fieldExpressions, fieldNullRates, fieldCollectionLengths, types, context.getKeys()));
  }

  @Override
  public void applyLimit(long limit) {
    this.numberOfRows = limit;
  }
}
