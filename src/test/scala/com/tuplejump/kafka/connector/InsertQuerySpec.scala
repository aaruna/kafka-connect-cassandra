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

import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord

class InsertQuerySpec extends AbstractFlatSpec {
  import KafkaCassandraProtocol._

  it should "convert a struct schema with single field" in {
    val topic = TopicConfig("test", "test.t1")
    val valueSchema = SchemaBuilder.struct.name("record").version(1).field("id", Schema.INT32_SCHEMA).build
    val value = new Struct(valueSchema).put("id", 1)
    val record = new SinkRecord(topic.topic, 1, null, null, valueSchema, value, 0)
    val result = CassandraWrite(record, topic)
    result.value should be("INSERT INTO test.t1(id) VALUES(1)")
  }

  it should "convert a struct schema with multiple fields" in {
    val topic = TopicConfig("test_kfk", "test.kfk")
    val valueSchema = SchemaBuilder.struct.name("record").version(1)
      .field("available", Schema.BOOLEAN_SCHEMA)
      .field("name", Schema.STRING_SCHEMA)
      .field("age", Schema.INT32_SCHEMA).build

    val value = new Struct(valueSchema).put("name", "user").put("available", false).put("age", 15)
    val record = new SinkRecord(topic.topic, 1, null, null, valueSchema, value, 0)
    val result = CassandraWrite(record, topic)
    result.value should be("INSERT INTO test.kfk(available,name,age) VALUES(false,'user',15)")
  }
}
