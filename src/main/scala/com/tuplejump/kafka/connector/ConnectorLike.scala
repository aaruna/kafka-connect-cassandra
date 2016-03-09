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

import org.apache.kafka.common.config.ConfigException

/** INTERNAL API. */
private[connector] trait ConnectorLike {

  protected var topics = TopicMap.emtpy

  /** Reads in the user provided configuration and fails fast if not valid.
    *
    * @param config the user config
    */
  protected def configure(config: Map[String, String]): Unit = {
    // work around for the enforced mutability in the kafka connect api
    topics = TopicMap(config)

    if (topics.values.isEmpty ) throw new ConfigException(s"""
      Unable to start ${getClass.getName} due to configuration error.
      `topics` property cannot be empty and there should be a `<topicName>_table`
      key whose value is `<keyspace>.<tableName>` for every topic.""")
  }
}

