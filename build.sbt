import Dependencies._

ThisBuild / scalaVersion     := "2.12.18"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"


// 1) Ejecuta en un JVM separado (muy importante para javaOptions del runtime)
run / fork := true

// 2) Flags necesarios para Java 17 (acceso a internals que Spark usa)
run / javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
)

// 3) Evita classloaders por capas (lo que sugiere el propio error)
ThisBuild / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat

run / outputStrategy := Some(StdoutOutput)

lazy val root = (project in file("."))
  .settings(
    name := "ejemploSpark",
    libraryDependencies += munit % Test
    // Source: https://mvnrepository.com/artifact/org.apache.spark/spark-core
  
  )

val versionSpark = "3.5.6"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % versionSpark,
 "org.apache.spark" %% "spark-sql" % versionSpark )