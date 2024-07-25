package org.apache.beam.sdk.io.gcp.bigquery.providers;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({
    "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class BigQueryDirectReadSchemaTransformProviderTest {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryDirectReadSchemaTransformProviderTest.class);

  public static void main(String[] args) {
    Pipeline p = Pipeline.create();
    PCollectionRowTuple output = PCollectionRowTuple.empty(p)
        .apply(
            new BigQueryDirectReadSchemaTransformProvider.BigQueryDirectReadSchemaTransform(
                BigQueryDirectReadSchemaTransformProvider.BigQueryDirectReadSchemaTransformConfiguration.builder()
                    .setTableSpec("cloud-teleport-testing.jkinard_test.test_data_types")
                    .build()));
        output.get("OUTPUT_ROWS").apply(MapElements.via(new SimpleFunction<Row, Boolean>(
            element -> {
              LOG.info("{}", element);
              return true;
            }) {}));
    p.run();
  }
}
