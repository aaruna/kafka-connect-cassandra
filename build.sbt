
import com.github.hochgi.sbt.cassandra.CassandraPlugin
import sbt._
import sbt.Keys._
import sbt.{EvictionWarningOptions, CrossVersion}
import de.heikoseeberger.sbtheader.license.Apache2_0

name := "cassandra-kafka-connector"

version := "0.0.4"

crossScalaVersions := Seq("2.11.7", "2.10.6")

crossVersion := CrossVersion.binary

scalaVersion := sys.props.getOrElse("scala.version", crossScalaVersions.value.head)

organization := "com.tuplejump"

description := "A Kafka Connect Cassandra Source and Sink connector."

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

cancelable in Global := true

de.heikoseeberger.sbtheader.HeaderPlugin.autoImport.headers := Map(
  "scala" -> Apache2_0("2016", "Tuplejump"),
  "conf"  -> Apache2_0("2016", "Tuplejump", "#")
)

lazy val cassandra = sys.props.getOrElse("cassandra.version","3.0.0")//3.0.4

libraryDependencies ++= Seq(
  "org.apache.kafka"       % "connect-api"           % "0.9.0.1",
  "com.datastax.cassandra" % "cassandra-driver-core" % cassandra,
  "org.scalatest"          %% "scalatest"            % "2.2.6"       % "test,it",
  "org.mockito"            % "mockito-core"          % "2.0.34-beta" % "test,it"
)

lazy val sourceEncoding = "UTF-8"

scalacOptions ++= Seq(
  "-Xfatal-warnings",
  "-deprecation",
  "-feature",
  "-language:_",
  "-unchecked",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-encoding", sourceEncoding
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
  "-encoding", sourceEncoding
)

evictionWarningOptions in update := EvictionWarningOptions.default
  .withWarnTransitiveEvictions(false)
  .withWarnDirectEvictions(false)
  .withWarnScalaVersionEviction(false)

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")

lazy val testScalastyle = taskKey[Unit]("testScalastyle")

import org.scalastyle.sbt.ScalastylePlugin
ScalastylePlugin.scalastyleFailOnError := true

testScalastyle := ScalastylePlugin.scalastyle.in(Test).toTask("").value

compileScalastyle := ScalastylePlugin.scalastyle.in(Compile).toTask("").value

import com.github.hochgi.sbt.cassandra._
CassandraPlugin.cassandraSettings

test in IntegrationTest <<= (test in IntegrationTest).dependsOn(startCassandra)

cassandraVersion := "2.2.2"

cassandraCqlInit := "src/it/resources/setup.cql"

lazy val testOptionsSettings = Tests.Argument(TestFrameworks.ScalaTest, "-oDF")

lazy val testConfigSettings = inConfig(Test)(Defaults.testTasks) ++
  inConfig(IntegrationTest)(Defaults.itSettings)

lazy val testSettings = testConfigSettings ++ cassandraSettings ++ Seq(
  fork in IntegrationTest := false,
  parallelExecution in Test := false,
  parallelExecution in IntegrationTest := false,
  testOptions in Test += testOptionsSettings,
  // default and stopCassandraAfterTests := true are not working
  testOptions in IntegrationTest += Tests.Cleanup( () => {
    val pid = cassandraPid.value
    println(s"Shutting down cassandra pid[$pid]")
    s"kill -9 $pid"!
  }),
  testOptions in IntegrationTest += testOptionsSettings,
  (internalDependencyClasspath in IntegrationTest) <<= Classpaths.concat(
    internalDependencyClasspath in IntegrationTest, exportedProducts in Test)
)

publishMavenStyle := false

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

lazy val root = (project in file(".")).settings(
    buildInfoKeys := Seq[BuildInfoKey](version),
    buildInfoPackage := "com.tuplejump.kafka.connector",
    buildInfoObject := "CassandraConnectorInfo")
  .settings(testSettings)
  .enablePlugins(BuildInfoPlugin)
  .enablePlugins(AutomateHeaderPlugin)
  .configs(IntegrationTest)
