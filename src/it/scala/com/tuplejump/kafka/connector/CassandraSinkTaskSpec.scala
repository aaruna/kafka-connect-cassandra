/*
 * Copyright 2016 Tuplejump
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tuplejump.kafka.connector

import scala.collection.JavaConverters._
import com.datastax.driver.core.Cluster
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}

class CassandraSinkTaskSpec extends AbstractFlatSpec {

  val topicName = "test_kv_topic"
  val tableName = "test.kv"
  val config = configFor((topicName, tableName))

  it should "start sink task" in {
    val sinkTask = new CassandraSinkTask()
    val mockContext = mock[SinkTaskContext]

    sinkTask.initialize(mockContext)

    sinkTask.start(config.asJava)
    sinkTask.session.isClosed should be(false)
    sinkTask.stop()
  }
  it should "save records in cassandra" in {
    val sinkTask = new CassandraSinkTask()
    val mockContext = mock[SinkTaskContext]

    sinkTask.initialize(mockContext)
    sinkTask.start(config.asJava)
    val valueSchema = SchemaBuilder.struct.name("record").version(1)
      .field("key", Schema.STRING_SCHEMA)
      .field("value", Schema.INT32_SCHEMA).build
    val value1 = new Struct(valueSchema).put("key", "pqr").put("value", 15)
    val value2 = new Struct(valueSchema).put("key", "abc").put("value", 17)

    val record1 = new SinkRecord(topicName, 1, SchemaBuilder.struct.build, "key", valueSchema, value1, 0)
    val record2 = new SinkRecord(topicName, 1, SchemaBuilder.struct.build, "key", valueSchema, value2, 0)

    sinkTask.put(List(record1, record2).asJava)

    sinkTask.stop()

    val session = CassandraCluster().connect
    val result = session.execute(s"select count(1) from $tableName").one()
    val rowCount = result.getLong(0)
    rowCount should be(2)
  }
}
