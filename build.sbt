import AssemblyKeys._

assemblySettings

name := "keystoneml"

version := "0.3.0-SNAPSHOT"

organization := "edu.berkeley.cs.amplab"

licenses := Seq("Apache 2.0" -> url("https://raw.githubusercontent.com/amplab/keystone/master/LICENSE"))

homepage := Some(url("http://keystone-ml.org"))

scalaVersion := "2.10.4"

parallelExecution in Test := false

libraryDependencies ++= Seq(
  "edu.arizona.sista" % "processors" % "3.0" exclude("ch.qos.logback", "logback-classic"),
  "edu.arizona.sista" % "processors" % "3.0" classifier "models",
  "org.slf4j" % "slf4j-api" % "1.7.2",
  "org.slf4j" % "slf4j-log4j12" % "1.7.2",
  "org.scalatest" %% "scalatest" % "1.9.1" % "test",
  "org.mockito" % "mockito-core" % "1.8.5",
  "org.apache.commons" % "commons-compress" % "1.7",
  "commons-io" % "commons-io" % "2.4",
  "org.scalanlp" % "breeze_2.10" % "0.11.2",
  "com.google.guava" % "guava" % "14.0.1",
  "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly(),
  "com.github.scopt" %% "scopt" % "3.3.0"
)

{
  val excludeSLF = ExclusionRule(organization = "org.slf4j")
  libraryDependencies ++= Seq(
    "org.scalanlp" %% "epic-parser-en-span" % "2015.2.19" excludeAll(excludeSLF),
    "org.scalanlp" %% "epic-pos-en" % "2015.2.19" excludeAll(excludeSLF),
    "org.scalanlp" %% "epic-ner-en-conll" % "2015.2.19" excludeAll(excludeSLF)
  )
}

{
  val defaultSparkVersion = "1.5.2"
  val sparkVersion =
    scala.util.Properties.envOrElse("SPARK_VERSION", defaultSparkVersion)
  val excludeHadoop = ExclusionRule(organization = "org.apache.hadoop")
  val excludeSpark = ExclusionRule(organization = "org.apache.spark")
  libraryDependencies ++= Seq(
    "org.apache.spark" % "spark-core_2.10" % sparkVersion excludeAll(excludeHadoop),
    "org.apache.spark" % "spark-mllib_2.10" % sparkVersion excludeAll(excludeHadoop),
    "org.apache.spark" % "spark-sql_2.10" % sparkVersion excludeAll(excludeHadoop),
    "edu.berkeley.cs.amplab" % "mlmatrix_2.10" % "0.1" excludeAll(excludeHadoop, excludeSpark)
  )
}

{
  val defaultHadoopVersion = "2.6.0"
  val hadoopVersion =
    scala.util.Properties.envOrElse("SPARK_HADOOP_VERSION", defaultHadoopVersion)
  libraryDependencies += "org.apache.hadoop" % "hadoop-client" % hadoopVersion
}

resolvers ++= Seq(
  "Local Maven Repository" at Path.userHome.asFile.toURI.toURL + ".m2/repository",
  "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Spray" at "http://repo.spray.cc"
)

resolvers += Resolver.sonatypeRepo("public")

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("javax", "servlet", xs @ _*)               => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".html"       => MergeStrategy.first
    case "application.conf"                                  => MergeStrategy.concat
    case "reference.conf"                                    => MergeStrategy.concat
    case "log4j.properties"                                  => MergeStrategy.first
    case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
    case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
    case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
    case _ => MergeStrategy.first
  }
}

test in assembly := {}

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

publishMavenStyle := true

publishArtifact in Test := false

// To publish on maven-central, all required artifacts must also be hosted on maven central.
pomIncludeRepository := { _ => false }

pomExtra := (
  <scm>
    <url>git@github.com:amplab/keystone.git</url>
    <connection>scm:git:git@github.com:amplab/keystone.git</connection>
  </scm>
  <developers>
    <developer>
      <id>etrain</id>
      <name>Evan Sparks</name>
      <url>http://etrain.github.io/about.html</url>
    </developer>
    <developer>
      <id>shivaram</id>
      <name>Shivaram Venkataraman</name>
      <url>http://shivaram.org</url>
    </developer>
    <developer>
      <id>tomerk</id>
      <name>Tomer Kaftan</name>
      <url>https://github.com/tomerk</url>
    </developer>
  </developers>
)
