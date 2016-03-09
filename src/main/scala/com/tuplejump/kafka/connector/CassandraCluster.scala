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

import java.net.InetAddress

import scala.util.Try
import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy
import com.datastax.driver.core.{ProtocolOptions, Session, Cluster, SocketOptions}
import org.apache.kafka.connect.errors.ConnectException

/** TODO refactor to control the # of cluster objs, sessions, cache sessions, etc. */
private[connector] final class CassandraCluster(val hosts: Set[InetAddress],
                                                val port: Int,
                                                val connectionTimeout: Int,
                                                val readTimeout: Int,
                                                val minReconnectDelay: Int,
                                                val maxReconnectDelay: Int,
                                                val compression: ProtocolOptions.Compression
                                               ) extends Serializable {

  sys.runtime.addShutdownHook(new Thread(s"Shutting down any open cassandra sessions.") {
    override def run(): Unit = shutdown()
  })

  private var cache: List[Session] = Nil

  /* Roadmap: .withRetryPolicy().withLoadBalancingPolicy().withAuthProvider().withSSL()*/
  lazy val cluster: Cluster = {
    val options = new SocketOptions()
      .setConnectTimeoutMillis(connectionTimeout)
      .setReadTimeoutMillis(readTimeout)

    Cluster.builder.addContactPoints(hosts.asJava).withPort(port)
      .withReconnectionPolicy(new ExponentialReconnectionPolicy(minReconnectDelay, maxReconnectDelay))
      .withCompression(compression)
      .withSocketOptions(options).build
  }

  /** Returns a new [[Session]]. */
  def connect: Session = try {
      val s = cluster.newSession()
      cache +:= s
      s
    } catch { case NonFatal(e) =>
      throw new ConnectException(
        s"Failed to connect to Cassandra cluster with host '$hosts' port '$port'", e)
    }

  def destroySession(session: Session): Unit = Try(session.close())

  /** Returns true if the [[Cluster]] is closed. */
  def isClosed: Boolean = cluster.isClosed

  private[connector] def shutdown(): Unit = {
    cache foreach destroySession
    Try(cluster.close())
  }
}

object CassandraCluster {

  /* Config to read from the user's config (Roadmap: or deploy environment or -D java system properties) */
  /** Cassandra hosts: contact points to connect to the Cassandra cluster.
    * A comma separated list of seed nodes may also be used: "127.0.0.1,192.168.0.1". */
  val HostPropertyName = "cassandra.connection.host"

  /** Cassandra native connection port. */
  val PortPropertyName = "cassandra.connection.port"

  /** Maximum period of time to attempt connecting to a node. */
  val ConnectionTimeoutPropertyName = "cassandra.connection.timeout.ms"

  /** Maximum period of time to wait for a read to return. */
  val ReadTimeoutPropertyName = "spark.cassandra.read.timeout.ms"

  /** Period of time to keep unused connections open. */
  val KeepAliveMillisPropertyName = "cassandra.connection.keep_alive.ms"

  /** Minimum period of time to wait before reconnecting to a dead node. */
  val MinReconnectionDelayPropertyName = "cassandra.connection.reconnection.delay.min.ms"

  /** Maximum period of time to wait before reconnecting to a dead node.
    * cassandra.connection.reconnection_delay_ms.max */
  val MaxReconnectionDelayPropertyName = "cassandra.connection.reconnection.delay.max.ms"

  /** Compression to use (LZ4, SNAPPY or NONE). */
  val CompressionPropertyName = "cassandra.connection.compression"

  val DefaultHosts = "127.0.0.1"
  val DefaultPort = ProtocolOptions.DEFAULT_PORT.toString
  val DefaultConnectionTimeoutMs = "5000"
  val DefaultReadTimeoutMs = "120000"
  val DefaultMinReconnectionDelayMs = "1000"
  val DefaultMaxReconnectionDelayMs = "60000"
  val DefaultCompression = ProtocolOptions.Compression.NONE

  /** Creates a new CassandraCluster instance with the default configuration for local use. */
  def apply(): CassandraCluster = apply(Map.empty)

  /** Creates a new CassandraCluster instance with the user configuration.
    * Default settings are used if not provided.
    *
    * @param config the user provided configuration
    */
  def apply(config: Map[String,String]): CassandraCluster = {

    def get(key: String, default: String): String = config.getOrElse(key, default)

    val _hosts = for {
      name    <- get(HostPropertyName, DefaultHosts).split(",").toSet[String]
      address <- resolve(name.trim)
    } yield address

    new CassandraCluster(
      hosts = _hosts,
      port = get(PortPropertyName, DefaultPort).toInt,
      connectionTimeout = get(ConnectionTimeoutPropertyName, DefaultConnectionTimeoutMs).toInt,
      readTimeout = get(ReadTimeoutPropertyName, DefaultReadTimeoutMs).toInt,
      minReconnectDelay = get(MinReconnectionDelayPropertyName, DefaultMinReconnectionDelayMs).toInt,
      maxReconnectDelay = get(MaxReconnectionDelayPropertyName, DefaultMaxReconnectionDelayMs).toInt,
      compression = config.get(CompressionPropertyName) map {
        case "snappy" => ProtocolOptions.Compression.SNAPPY
        case "lz4" => ProtocolOptions.Compression.LZ4
      } getOrElse DefaultCompression
    )
  }

  private def resolve(hostName: String): Option[InetAddress] =
    try Some(InetAddress.getByName(hostName)) catch {
      case NonFatal(e) => None
    }
}
