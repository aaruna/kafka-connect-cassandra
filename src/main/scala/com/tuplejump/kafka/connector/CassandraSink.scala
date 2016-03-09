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

import java.util.{List => JList, Map => JMap}

import scala.collection.immutable
import scala.collection.JavaConverters._
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.errors.ConnectException

class CassandraSink extends SinkConnector with ConnectorLike {

  override def taskClass: Class[_ <: Task] = classOf[CassandraSinkTask]

  override def taskConfigs(maxTasks: Int): JList[JMap[String, String]] =
    List.fill(maxTasks)(topics.config.asJava).asJava

  override def stop(): Unit = ()

  override def start(config: JMap[String, String]): Unit =
    try configure(immutable.Map.empty[String,String] ++ config.asScala) catch {
      case e: ConfigException => throw new ConnectException(e)
    }

  override def version: String = CassandraConnectorInfo.version

}
