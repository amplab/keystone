import AssemblyKeys._

assemblySettings

name := "keystone"

version := "0.1"

organization := "edu.berkeley.cs.amplab"

scalaVersion := "2.10.4"

parallelExecution in Test := false

lazy val execMakefile = taskKey[Unit]("Execute the makefile")

execMakefile := {
    "make" !
}

compile <<= (compile in Compile) dependsOn execMakefile

{
  val excludeHadoop = ExclusionRule(organization = "org.apache.hadoop")
  libraryDependencies ++= Seq(
    "edu.arizona.sista" % "processors" % "3.0" exclude("ch.qos.logback", "logback-classic"),
    "edu.arizona.sista" % "processors" % "3.0" classifier "models",
    "org.slf4j" % "slf4j-api" % "1.7.2",
    "org.slf4j" % "slf4j-log4j12" % "1.7.2",
    "org.scalatest" %% "scalatest" % "1.9.1" % "test",
    "org.apache.spark" % "spark-core_2.10" % "1.3.1" excludeAll(excludeHadoop),
    "org.apache.spark" % "spark-mllib_2.10" % "1.3.1" excludeAll(excludeHadoop),
    "org.apache.spark" % "spark-sql_2.10" % "1.3.1" excludeAll(excludeHadoop),
    "org.apache.commons" % "commons-compress" % "1.7",
    "commons-io" % "commons-io" % "2.4",
    "org.scalanlp" % "breeze_2.10" % "0.11.2",
    "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly(),
    "edu.berkeley.cs.amplab" % "mlmatrix" % "0.1" from "https://s3-us-west-1.amazonaws.com/amp-ml-matrix/2.10/mlmatrix_2.10-0.1.jar",
    "com.github.scopt" %% "scopt" % "3.3.0"
  )
}

{
  val defaultHadoopVersion = "2.0.0-mr1-cdh4.2.0"
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

resolvers += "Amplab ml-matrix repo" at "https://s3-us-west-1.amazonaws.com/"

resolvers += Resolver.sonatypeRepo("public")

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("javax", "servlet", xs @ _*)           => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".html"   => MergeStrategy.first
    case "application.conf"                              => MergeStrategy.concat
    case "reference.conf"                                => MergeStrategy.concat
    case "log4j.properties"                              => MergeStrategy.first
    case m if m.toLowerCase.endsWith("manifest.mf")      => MergeStrategy.discard
    case m if m.toLowerCase.matches("meta-inf.*\\.sf$")  => MergeStrategy.discard
    case _ => MergeStrategy.first
  }
}

test in assembly := {}
