name := "spark-avro"

organization := "com.databricks"

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.10.6", "2.11.8")

spName := "databricks/spark-avro"

sparkVersion := "1.6.1"

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.getOrElse("spark.testVersion", sparkVersion.value)

val testHadoopVersion = settingKey[String]("The version of Hadoop to test against.")

testHadoopVersion := sys.props.getOrElse("hadoop.testVersion", "2.2.0")

spAppendScalaVersion := true

spIncludeMaven := true

spIgnoreProvided := true

sparkComponents := Seq("sql")

libraryDependencies ++= Seq(
  "org.apache.avro" % "avro" % "1.8.1" exclude("org.mortbay.jetty", "servlet-api"),
  "org.apache.avro" % "avro-mapred" % "1.8.1"  % "provided" classifier("hadoop2") exclude("org.mortbay.jetty", "servlet-api"),
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "commons-io" % "commons-io" % "2.4" % "test"
)

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % testHadoopVersion.value % "test",
  "org.apache.spark" %% "spark-core" % testSparkVersion.value % "test" exclude("org.apache.hadoop", "hadoop-client"),
  "org.apache.spark" %% "spark-sql" % testSparkVersion.value % "test" exclude("org.apache.hadoop", "hadoop-client")
)

// Display full-length stacktraces from ScalaTest:
testOptions in Test += Tests.Argument("-oF")

ScoverageSbtPlugin.ScoverageKeys.coverageHighlighting := {
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


