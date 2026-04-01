name := "ecommerce-batch"
version := "1.0"
scalaVersion := "2.12.15"

resolvers += "Google Maven" at "https://maven.google.com"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.3.0" % "provided",
  "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.32.2"
)