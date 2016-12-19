/*
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.storage.hive;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.storage.partitioner.Partitioner;

public abstract class HiveUtil {

  protected final String url;
  protected final String topicsDir;
  protected final AvroData avroData;
  protected final HiveMetaStore hiveMetaStore;

  public HiveUtil(AbstractConfig connectorConfig, AvroData avroData, HiveMetaStore hiveMetaStore) {
    String urlKey;

    urlKey = connectorConfig.getString(HiveConfig.STORE_URL_CONFIG);
    if (urlKey == null || urlKey.equals(HiveConfig.STORE_URL_DEFAULT)) {
      urlKey = connectorConfig.getString(HiveConfig.HDFS_URL_CONFIG);
    }

    this.url = urlKey;
    this.topicsDir = connectorConfig.getString(HiveConfig.TOPICS_DIR_CONFIG);
    this.avroData = avroData;
    this.hiveMetaStore = hiveMetaStore;
  }

  public abstract void createTable(String database, String tableName, Schema schema, Partitioner partitioner);

  public abstract void alterSchema(String database, String tableName, Schema schema);

  public String hiveDirectoryName(String url, String topicsDir, String topic) {
    return url + "/" + topicsDir + "/" + topic + "/";
  }
}
