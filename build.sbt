

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.getOrElse("spark.testVersion", sparkVersion.value)

val testHadoopVersion = settingKey[String]("The version of Hadoop to test against.")

testHadoopVersion := sys.props.getOrElse("hadoop.testVersion", "2.2.0")

val testAvroVersion = settingKey[String]("The version of Avro to test against.")

testAvroVersion := sys.props.getOrElse("avro.testVersion", "1.7.6")

val testAvroMapredVersion = settingKey[String]("The version of avro-mapred to test against.")

testAvroMapredVersion := sys.props.getOrElse("avroMapred.testVersion", "1.7.7")

// curator leads to conflicting guava dependencies
val curatorExclusion = ExclusionRule(organization = "org.apache.curator")

lazy val sparkDependencies = settingKey[Seq[ModuleID]]("Spark dependencies")
sparkDependencies in ThisBuild :=  Seq(
  "org.apache.hadoop" % "hadoop-client" % testHadoopVersion.value % "provided,test" excludeAll(curatorExclusion),
  "org.apache.spark" %% "spark-core" % testSparkVersion.value % "provided,test" exclude("org.apache.hadoop", "hadoop-client") excludeAll(curatorExclusion) exclude("org.scalatest", "scalatest_2.11"),
  "org.apache.spark" %% "spark-sql" % testSparkVersion.value % "provided,test" exclude("org.apache.hadoop", "hadoop-client") excludeAll(curatorExclusion) exclude("org.scalatest", "scalatest_2.11")
)

lazy val avroDependencies = settingKey[Seq[ModuleID]]("Avro dependencies")
avroDependencies in ThisBuild := Seq(
  "org.apache.avro" % "avro" % testAvroVersion.value % "test" exclude("org.mortbay.jetty", "servlet-api"),
  "org.apache.avro" % "avro-mapred" % testAvroMapredVersion.value  % "test" classifier("hadoop2") exclude("org.mortbay.jetty", "servlet-api")
)

lazy val root = (project in file(".")) settings(
  name := "spark-avro",
  libraryDependencies ++= sparkDependencies.value ++ avroDependencies.value,
  inThisBuild(Seq(
    organization := "com.databricks",
    scalaVersion := "2.11.8",
    crossScalaVersions := Seq("2.10.6", "2.11.8"),

    sparkVersion := "2.1.0",
    sparkComponents := Seq("sql"),

    spName := "databricks/spark-avro",
    spAppendScalaVersion := true,
    spIncludeMaven := true,
    spIgnoreProvided := true,

    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-api" % "1.7.5",
      // Kryo is provided by Spark, but we need this here in order to be able to import the @DefaultSerializer annotation:
      "com.esotericsoftware" % "kryo-shaded" % "3.0.3" % "provided",
      "org.scalatest" %% "scalatest" % "3.0.5" % "test",
      "org.scalacheck" %% "scalacheck" % "1.13.4" % "test",
      "commons-io" % "commons-io" % "2.4" % "test"
    ),
    dependencyOverrides += "org.scalatest" %% "scalatest" % "3.0.5",

    // Display full-length stacktraces from ScalaTest:
    testOptions in Test += Tests.Argument("-oF"),
    scalacOptions ++= Seq("-target:jvm-1.7"),
    javacOptions ++= Seq("-source", "1.7", "-target", "1.7"),

    coverageHighlighting := (scalaBinaryVersion.value != "2.10"),

    EclipseKeys.eclipseOutput := Some("target/eclipse")
  ))
)

lazy val `spark-avro-functions` = (project in file("spark-avro-functions")) settings(
  // fork will avoid creation of multiple spark contexts while running tests
  fork in Test := true,
  libraryDependencies ++= sparkDependencies.value ++ avroDependencies.value
) dependsOn(root)

/********************
 * Release settings *
 ********************/

publishMavenStyle in ThisBuild := true

releaseCrossBuild in ThisBuild := true

licenses in ThisBuild += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

releasePublishArtifactsAction in ThisBuild := PgpKeys.publishSigned.value

pomExtra in ThisBuild :=
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
releaseProcess in ThisBuild := Seq[ReleaseStep](
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
