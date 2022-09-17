package com.github.knaufk.flink.faker;

import static com.github.knaufk.flink.faker.FlinkFakerTableSourceFactory.UNLIMITED_ROWS;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.data.RowData;

public class FlinkFakerTableSource
    implements ScanTableSource, LookupTableSource, SupportsLimitPushDown {

  private final FieldInfo[] fieldInfos;
  private ResolvedSchema schema;
  private long rowsPerSecond;
  private long numberOfRows;

  public FlinkFakerTableSource(
      FieldInfo[] fieldInfos, ResolvedSchema schema, long rowsPerSecond, long numberOfRows) {
    this.fieldInfos = fieldInfos;
    this.schema = schema;
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
    //    return SourceFunctionProvider.of(
    //      new FlinkFakerGenerator(fieldInfos, rowsPerSecond, numberOfRows), isBounded);

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

        return sequence.flatMap(new FlinkFakerGenerator(fieldInfos, numberOfRows));
      }

      @Override
      public boolean isBounded() {
        return isBounded;
      }
    };
  }

  @Override
  public DynamicTableSource copy() {
    return new FlinkFakerTableSource(fieldInfos, schema, rowsPerSecond, numberOfRows);
  }

  @Override
  public String asSummaryString() {
    return "FlinkFakerSource";
  }

  @Override
  public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
    return TableFunctionProvider.of(new FlinkFakerLookupFunction(fieldInfos, context.getKeys()));
  }

  @Override
  public void applyLimit(long limit) {
    this.numberOfRows = limit;
  }
}
