import sbt._
import sbt.Keys._
import sbt.{EvictionWarningOptions, CrossVersion}
import de.heikoseeberger.sbtheader.license.Apache2_0

name := "cassandra-kafka-connector"

version := "0.0.3"

crossScalaVersions := Seq("2.11.7", "2.10.6")

crossVersion := CrossVersion.binary

scalaVersion := sys.props.getOrElse("scala.version", crossScalaVersions.value.head)

organization := "com.tuplejump"

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

cancelable in Global := true

de.heikoseeberger.sbtheader.HeaderPlugin.autoImport.headers := Map(
  "scala" -> Apache2_0("2016", "Tuplejump"),
  "conf"  -> Apache2_0("2016", "Tuplejump", "#")
)

libraryDependencies ++= Seq(
  "org.apache.kafka"       % "connect-api"           % "0.9.0.1",
  "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.9",
  "org.scalatest"          %% "scalatest"            % "2.2.4"       % "test",
  "org.mockito"            % "mockito-core"          % "2.0.34-beta" % "test"
)

scalacOptions ++= Seq(
  "-Xfatal-warnings",
  "-deprecation",
  "-feature",
  "-language:_",
  "-unchecked",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-encoding", "UTF-8"
)

scalacOptions ++= (
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, minor)) if minor < 11 => Seq.empty
    case _ => Seq("-Ywarn-unused-import")
  })

javacOptions ++= Seq(
  "-Xmx1G",
  "-Xlint:unchecked",
  "-Xlint:deprecation",
  "-encoding", "UTF-8"
)

evictionWarningOptions in update := EvictionWarningOptions.default
  .withWarnTransitiveEvictions(false)
  .withWarnDirectEvictions(false)
  .withWarnScalaVersionEviction(false)

import com.github.hochgi.sbt.cassandra._
CassandraPlugin.cassandraSettings

test in Test <<= (test in Test).dependsOn(startCassandra)

cassandraVersion := "2.2.2"

cassandraCqlInit := "src/test/resources/setup.cql"

lazy val root = (project in file(".")).settings(
    buildInfoKeys := Seq[BuildInfoKey](version),
    buildInfoPackage := "com.tuplejump.kafka.connector",
    buildInfoObject := "CassandraConnectorInfo")
  .enablePlugins(BuildInfoPlugin)
  .enablePlugins(AutomateHeaderPlugin)

pomExtra :=
  <scm>
    <url>git@github.com:tuplejump/kafka-connector.git</url>
    <connection>scm:git:git@github.com:tuplejump/kafka-connector.git</connection>
  </scm>
    <developers>
      <developer>
        <id>Shiti</id>
        <name>Shiti Saxena</name>
        <url>https://twitter.com/eraoferrors</url>
      </developer>
      <developer>
        <id>helena</id>
        <name>Helena Edelson</name>
        <url>https://twitter.com/helenaedelson</url>
      </developer>
    </developers>