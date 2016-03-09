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

import java.util.Date

import scala.collection.JavaConverters._
import org.apache.kafka.connect.data.Schema.Type._
import org.apache.kafka.connect.data.{Timestamp, Schema, Struct}
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.sink.SinkRecord

object KafkaCassandraProtocol {

  /** The Cassandra `keyspace.table`. */
  type QueryNamespace = String

  /** The Kafka topic name. */
  type TopicName = String

  /** The formatted key to get topic to QueryNamespace mappings from config. */
  type Key = String

  /** Represents a map of topics to cassandra keyspace.
    * INTERNAL API.
    *
    * @param topic the kafka `topic` name
    * @param key the key in the user config for the `topic`
    * @param namespace the cassandra `keyspace.table`
    */
  private[connector] final case class TopicConfig(topic: TopicName, key: Key, namespace: QueryNamespace)

  /** INTERNAL API. */
  private[connector] object TopicConfig {

    val TopicNameElementSize = 2
    val KeyFormat = "_table"

    def apply(topic: TopicName)(implicit config: Map[String,String]): Option[TopicConfig] =
      config.get(keyFor(topic)) collect {
        case value if valid(value) => apply(topic,value)
      }

    def apply(topic: TopicName, value: QueryNamespace):TopicConfig =
      TopicConfig(topic, keyFor(topic), value)

    /** At this point `s` is known to not be empty but it must
      * have the expected format, which at a minimum is `a.b`. */
    private def valid(s: String): Boolean =
      s.nonEmpty && s.contains(".") && s.length >= TopicNameElementSize &&
        s.split("\\.").forall(_.nonEmpty)

    def keyFor(topic: String): Key =
      topic.trim + KeyFormat

  }

  sealed trait Protocol {
    def value: String
  }

  private[connector] final case class CassandraWrite private(value: String) extends Protocol

  /** INTERNAL API. */
  private[connector] object CassandraWrite {

    //TODO use keySchema, partition and kafkaOffset
    //TODO: README - which types are currently supported + roadmap for supported types
    def apply(sinkRecord: SinkRecord, topic: TopicConfig): CassandraWrite = {
      val valueSchema = sinkRecord.valueSchema
      val columnNames = valueSchema.fields.asScala.map(_.name).toSet
      val columnValues = valueSchema.`type`() match {
        case STRUCT =>
          val result: Struct = sinkRecord.value.asInstanceOf[Struct]
          columnNames.map(schema(valueSchema, result, _)).mkString(",")
        case other =>
          throw new DataTypeException(
            s"Unable to create insert statement with unsupported value schema type $other.")
      }
      CassandraWrite(s"INSERT INTO ${topic.namespace}(${columnNames.mkString(",")}) VALUES($columnValues)")
    }

    //TODO ensure all types are supported
    private def schema(valueSchema: Schema, result: Struct, col: String) =
      valueSchema.field(col).schema match {
        case x if x.`type`() == Schema.STRING_SCHEMA.`type`() =>
          s"'${result.get(col).toString}'"
        case x if x.name() == Timestamp.LOGICAL_NAME =>
          val time = Timestamp.fromLogical(x, result.get(col).asInstanceOf[Date])//can we use Joda instead?
          s"$time"
        case y =>
          result.get(col)
      }
  }
}

class DataTypeException(msg: String) extends DataException(msg)