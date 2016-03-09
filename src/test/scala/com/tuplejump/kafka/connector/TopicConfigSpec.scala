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

class TopicConfigSpec extends AbstractSpec {
  import KafkaCassandraProtocol._,TopicConfig._

  "A TopicConfig" must {
    "validate and create a new TopicConfig from a valid topic and keyspace.table name" in {
      val t1 = "topic1"
      val n1 = "ks1.t1"
      val tconfig = TopicConfig(t1, n1)
      tconfig.key should be (keyFor(t1))
      tconfig.topic should be (t1)
      tconfig.namespace should be (n1)
    }

    "validate and not create new TopicConfig from invalid configuration" in {
      Set(("a", "b."), ("a", "b"), ("a", ".b"), ("a", "."), ("a", "ab"), ("a", "")) foreach {
        case (k,v) => TopicConfig("topic")(configFor((k,v))).isEmpty should be (true)
      }
    }

    "validate and create new TopicConfigs for multiple valid entries" in {
      val t1 = "topic1"
      val n1 = "ks1.t1"
      val t2 = "topic2"
      val n2 = "ks1.t2"

      val topics = TopicMap(configFor((t1, n1),(t2, n2))).values
      topics.size should be (2)
      topics.contains(TopicConfig(t1, n1)) should be (true)
      topics.contains(TopicConfig(t2, n2)) should be (true)
      topics foreach { c =>
        c.key should be (keyFor(c.topic))
      }
    }
  }
}
