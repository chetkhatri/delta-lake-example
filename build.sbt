name := "delta_lake_example"

version := "0.1"

scalaVersion := "2.12.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
libraryDependencies += "io.delta" %% "delta-core" % "0.7.0"


