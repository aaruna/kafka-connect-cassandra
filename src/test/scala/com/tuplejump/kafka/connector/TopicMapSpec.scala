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

class TopicMapSpec extends AbstractSpec {
  import KafkaCassandraProtocol._,TopicConfig._

  "A TopicMap" must {
    "validate and create new TopicConfigs from valid configurations" in {
      TopicMap(
        configFor(("a", "k.t"), ("a.b", "k.t")))
        .values.size should be (2)
    }
    "validate and not create new TopicConfigs from invalid configurations" in {
      TopicMap(
        configFor(("a", "b."), ("a", "b"), ("a", ".b"), ("a", "."), ("a", "ab"), ("a", "")))
        .values.isEmpty should be (true)
    }
    "validate and create new TopicConfigs for multiple valid entries" in {
      val t1 = "topic1"
      val n1 = "ks1.t1"
      val t2 = "topic2"
      val n2 = "ks1.t2"

      val topics = TopicMap(configFor((t1, n1),(t2, n2)))
      topics.values.size should be (2)
      topics.values.contains(TopicConfig(t1, n1)) should be (true)
      topics.values.contains(TopicConfig(t2, n2)) should be (true)
      topics.values foreach { c =>
        c.key should be (keyFor(c.topic))
      }
    }
  }
}
