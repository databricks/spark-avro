name := "spark-avro"

version := "1.0.0"

organization := "com.databricks"

scalaVersion := "2.10.5"

crossScalaVersions := Seq("2.10.5", "2.11.7")

spName := "databricks/spark-avro"

sparkVersion := "1.3.0"

sparkComponents += "sql"

spAppendScalaVersion := true

spIncludeMaven := true

libraryDependencies += "org.apache.avro" % "avro" % "1.7.6" exclude("org.mortbay.jetty", "servlet-api")

libraryDependencies += "org.apache.avro" % "avro-mapred" % "1.7.6" % "provided" exclude("org.mortbay.jetty", "servlet-api")

publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (version.value.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

pomExtra := (
  <url>https://github.com/databricks/spark-avro</url>
  <licenses>
    <license>
      <name>Apache License, Verision 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
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
      <id>vlyubin</id>
      <name>Volodymyr Lyubinets</name>
      <url>https://github.com/vlyubin</url>
    </developer>
  </developers>)

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"

ScoverageSbtPlugin.ScoverageKeys.coverageHighlighting := {
  if (scalaBinaryVersion.value == "2.10") false
  else false
}
