/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.connect.storage.partitioner;

import io.confluent.connect.storage.StorageSinkTestBase;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class TimestampExtractorTest extends StorageSinkTestBase {
  private final String TIMESTAMP_FIELD_NAME = "timestamp";

  @Test
  public void testRecordFieldTimestampExtractor() {
    TimestampExtractor extractor = new TimeBasedPartitioner.RecordFieldTimestampExtractor();
    HashMap<String, Object> props = new HashMap<>();
    props.put(PartitionerConfig.TIMESTAMP_FIELD_NAME_CONFIG, TIMESTAMP_FIELD_NAME);
    extractor.configure(props);
    long timestamp = 1410478343000L;
    SinkRecord record;


    record = createSinkRecord(timestamp, Schema.INT64_SCHEMA);
    assertEquals(timestamp, (long) extractor.extract(record));

    record = createSinkRecord((float) (timestamp / 1000), Schema.FLOAT32_SCHEMA);
    assertEquals(timestamp, (long) extractor.extract(record));

    record = createSinkRecord((double) (timestamp + 0.1), Schema.FLOAT64_SCHEMA);
    assertEquals(timestamp, (long) extractor.extract(record));

    record = createSinkRecord((int) (timestamp / 1000), Schema.INT32_SCHEMA);
    assertEquals(timestamp, (long) extractor.extract(record));

    record = createSinkRecord("2014-09-11T23:32:23Z", Schema.STRING_SCHEMA);
    assertEquals(timestamp, (long) extractor.extract(record));

    record = createSinkRecord("2014-09-11T23:32:23.000Z", Schema.STRING_SCHEMA);
    assertEquals(timestamp, (long) extractor.extract(record));
  }

  private Schema createSchemaWithTimestampField(Schema timestampSchema) {
    return SchemaBuilder.struct().name("record").version(1)
        .field(TIMESTAMP_FIELD_NAME, timestampSchema)
        .build();
  }

  private Struct createRecordWithTimestampField(Schema newSchema, Object timestamp) {
    return new Struct(newSchema)
        .put(TIMESTAMP_FIELD_NAME, timestamp);
  }

  private SinkRecord createSinkRecord(Object timestamp, Schema timestampSchema) {
    Schema schema = createSchemaWithTimestampField(timestampSchema);
    Struct record = createRecordWithTimestampField(schema, timestamp);
    return new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, schema, record, 0L,
        null, TimestampType.CREATE_TIME);
  }
}
