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

import java.util.{Map => JMap}

import scala.collection.JavaConverters._
import org.apache.kafka.connect.sink.SinkConnector
import org.apache.kafka.connect.errors.ConnectException

class CassandraSinkSpec extends AbstractFlatSpec {
  import KafkaCassandraProtocol.TopicConfig._

  private val MultipleTopics: String = "test1,test2"

  private val sinkProperties: JMap[String, String] =
    (commonConfig ++ Map(SinkConnector.TOPICS_CONFIG -> MultipleTopics,
      keyFor("test1") -> "test.test1",
      keyFor("test2") -> "test.test1")).asJava

  it should "fail on start if no configurations exist" in {
    an[ConnectException] should be thrownBy {
      new CassandraSink().start(EmptyProperties.asJava)
    }
  }
  it should "fail on start if no topics are configured" in {
    an[ConnectException] should be thrownBy {
      new CassandraSink().start(Map(SinkConnector.TOPICS_CONFIG -> MultipleTopics).asJava)
    }
  }
  it should "fail on start if configurations are invalid" in {
    an[ConnectException] should be thrownBy {
      new CassandraSink().start(Map(SinkConnector.TOPICS_CONFIG -> "test", keyFor("test") -> "test").asJava)
    }
  }
  it should "have taskConfigs" in {
    val cassandraSink: CassandraSink = new CassandraSink
    cassandraSink.start(sinkProperties)
    var taskConfigs = cassandraSink.taskConfigs(1)
    taskConfigs.size should be(1)
    taskConfigs.get(0).get(CassandraCluster.HostPropertyName) should be(CassandraCluster.DefaultHosts)
    taskConfigs = cassandraSink.taskConfigs(2)
    taskConfigs.size should be(2)
  }
}
