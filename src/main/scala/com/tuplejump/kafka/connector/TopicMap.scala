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

import scala.collection.immutable
import org.apache.kafka.connect.sink.SinkConnector
import com.tuplejump.kafka.connector.KafkaCassandraProtocol._

/** INTERNAL API. */
private[connector] final class TopicMap private(val config: immutable.Map[String,String],
                                                val values: immutable.Set[TopicConfig]) {

  /** Returns the [[TopicConfig]] for the `topic` if exists. */
  def find(topic: TopicName): Option[TopicConfig] =
    values.find(_.topic == topic)

}

/** INTERNAL API. */
object TopicMap {

  val TopicSeparator = ","

  val emtpy = new TopicMap(Map.empty,Set.empty)

  def apply(config: Map[String,String]): TopicMap = {
    val topics = config.get(SinkConnector.TOPICS_CONFIG) collect {
      case s if s.trim.nonEmpty => topicConfigs(s)(config)
    } getOrElse Set.empty

    new TopicMap(config, topics)
  }

  private[connector] def topicConfigs(s: String)(implicit config: Map[String,String]): Set[TopicConfig] =
    s.trim.split(TopicSeparator).flatMap(TopicConfig(_)(config)).toSet

}
