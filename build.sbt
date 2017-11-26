import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.btl",
      scalaVersion := "2.11.8",
      version      := "0.1.0"
    )),
    // name determines the name of packaged jar
    name := "SparklingBreakpoint",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
  )
