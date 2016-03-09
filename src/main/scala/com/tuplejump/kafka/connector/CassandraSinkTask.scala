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

import java.util.{Collection => JCollection, Map => JMap}

import scala.collection.immutable
import scala.collection.JavaConverters._
import com.datastax.driver.core.Session
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}
import org.apache.kafka.connect.errors.ConnectException

class CassandraSinkTask extends SinkTask with ConnectorLike {
  import KafkaCassandraProtocol._

  private var cluster: Option[CassandraCluster] = None

  private var _session: Option[Session] = None

  sys.runtime.addShutdownHook(new Thread(s"Shutting down any open cassandra sessions.") {
    override def run(): Unit = shutdown()
  })

  def session: Session = _session.getOrElse(throw new IllegalStateException(
    "Sink has not been started yet or is not configured properly to connect to a cluster."))

  override def stop(): Unit = shutdown()

  override def put(records: JCollection[SinkRecord]): Unit =
    records.asScala.foreach { record =>
      topics.find(record.topic) match {
        case Some(topic) =>
          val command = CassandraWrite(record, topic)
          session.execute(command.value)
        case other =>
          throw new ConnectException("Failed to get cassandra session.")
      }
    }

  //This method is not relevant as we insert every received record in Cassandra
  override def flush(offsets: JMap[TopicPartition, OffsetAndMetadata]): Unit = ()

  override def start(properties: JMap[String, String]): Unit = {
    val config = immutable.Map.empty[String,String] ++ properties.asScala
    configure(config)

    val cassandraCluster = CassandraCluster(config)
    _session = Some(cassandraCluster.connect)
    cluster = Some(cassandraCluster)
  }

  override def version: String = CassandraConnectorInfo.version

  private def shutdown(): Unit = {
    cluster foreach(_.shutdown())
  }
}
