name := "DataFlow"

version := "1.0.0"

scalaVersion := "2.11.12"

scalacOptions += "-Ypartial-unification"

useGpg := true
credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials")
publishConfiguration := publishConfiguration.value.withOverwrite(true)

// https://mvnrepository.com/artifact/org.scala-lang/scala-library
libraryDependencies += "org.scala-lang" % "scala-library" % "2.11.12"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.1"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.1"
// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.2.1" % "provided"
// https://mvnrepository.com/artifact/com.typesafe/config
libraryDependencies += "com.typesafe" % "config" % "1.3.2"
// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0-SNAP4"
// https://mvnrepository.com/artifact/com.github.pureconfig/pureconfig
libraryDependencies += "com.github.pureconfig" %% "pureconfig" % "0.10.0"
// https://mvnrepository.com/artifact/org.typelevel/cats-core
libraryDependencies += "org.typelevel" %% "cats-core" % "1.5.0"
// https://mvnrepository.com/artifact/org.slf4j/slf4j-api
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.25"
// https://mvnrepository.com/artifact/com.github.scopt/scopt
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.0-RC2"



ThisBuild / organization := "io.github.vbounyasit"
ThisBuild / organizationName := "data-projects"

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("http://github.com/vbounyasit/DataFlow/tree/master"),
    "scm:git:git://github.com/vbounyasit/DataFlow.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id    = "vbounyasit",
    name  = "Vibert Bounyasit",
    email = "vibert.bounyasit@gmail.com",
    url   = url("https://github.com/vbounyasit")
  )
)

ThisBuild / description := "A Scala ETL Framework based on Apache Spark for Data engineers."
ThisBuild / licenses := List("The Apache Software License, Version 2.0" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://github.com/vbounyasit/DataFlow"))

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
ThisBuild / publishMavenStyle := true