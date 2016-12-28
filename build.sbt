name := "spark-avro"

organization := "com.databricks"

scalaVersion := "2.11.7"

crossScalaVersions := Seq("2.10.5", "2.11.7")

spName := "databricks/spark-avro"

sparkVersion := "2.1.0"

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.getOrElse("spark.testVersion", sparkVersion.value)

val testHadoopVersion = settingKey[String]("The version of Hadoop to test against.")

testHadoopVersion := sys.props.getOrElse("hadoop.testVersion", "2.2.0")

val testAvroVersion = settingKey[String]("The version of Avro to test against.")

testAvroVersion := sys.props.getOrElse("avro.testVersion", "1.7.6")

val testAvroMapredVersion = settingKey[String]("The version of avro-mapred to test against.")

testAvroMapredVersion := sys.props.getOrElse("avroMapred.testVersion", "1.7.7")

spAppendScalaVersion := true

spIncludeMaven := true

spIgnoreProvided := true

sparkComponents := Seq("sql")

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.5",
  "org.apache.avro" % "avro" % "1.7.6" exclude("org.mortbay.jetty", "servlet-api"),
  "org.apache.avro" % "avro-mapred" % "1.7.7"  % "provided" classifier("hadoop2") exclude("org.mortbay.jetty", "servlet-api"),
  // Kryo is provided by Spark, but we need this here in order to be able to import the @DefaultSerializer annotation:
  "com.esotericsoftware" % "kryo-shaded" % "3.0.3" % "provided",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "commons-io" % "commons-io" % "2.4" % "test"
)

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % testHadoopVersion.value % "test",
  "org.apache.spark" %% "spark-core" % testSparkVersion.value % "test" exclude("org.apache.hadoop", "hadoop-client"),
  "org.apache.spark" %% "spark-sql" % testSparkVersion.value % "test" exclude("org.apache.hadoop", "hadoop-client"),
  "org.apache.avro" % "avro" % testAvroVersion.value % "test" exclude("org.mortbay.jetty", "servlet-api"),
  "org.apache.avro" % "avro-mapred" % testAvroMapredVersion.value  % "test" classifier("hadoop2") exclude("org.mortbay.jetty", "servlet-api")
)

// Display full-length stacktraces from ScalaTest:
testOptions in Test += Tests.Argument("-oF")

scalacOptions ++= Seq("-target:jvm-1.7")

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

coverageHighlighting := {
  if (scalaBinaryVersion.value == "2.10") false
  else true
}

EclipseKeys.eclipseOutput := Some("target/eclipse")

/********************
 * Release settings *
 ********************/

publishMavenStyle := true

releaseCrossBuild := true

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

releasePublishArtifactsAction := PgpKeys.publishSigned.value

pomExtra :=
  <url>https://github.com/databricks/spark-avro</url>
  <scm>
    <url>git@github.com:databricks/spark-avro.git</url>
    <connection>scm:git:git@github.com:databricks/spark-avro.git</connection>
  </scm>
  <developers>
    <developer>
      <id>marmbrus</id>
      <name>Michael Armbrust</name>
      <url>https://github.com/marmbrus</url>
    </developer>
    <developer>
      <id>JoshRosen</id>
      <name>Josh Rosen</name>
      <url>https://github.com/JoshRosen</url>
    </developer>
    <developer>
      <id>vlyubin</id>
      <name>Volodymyr Lyubinets</name>
      <url>https://github.com/vlyubin</url>
    </developer>
  </developers>

bintrayReleaseOnPublish in ThisBuild := false

import ReleaseTransformations._

// Add publishing to spark packages as another step.
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  setNextVersion,
  commitNextVersion,
  pushChanges,
  releaseStepTask(spPublish)
)
