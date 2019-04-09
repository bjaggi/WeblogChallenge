version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"


resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
   "org.apache.spark" %% "spark-sql" % sparkVersion,
   "org.apache.spark" %% "spark-mllib" % sparkVersion,
   "org.apache.spark" %% "spark-streaming" % sparkVersion,
   "org.apache.spark" %% "spark-hive" % sparkVersion, 
   //"mysql" % "mysql-connector-java" % "5.1.6"



  "junit" % "junit" % "4.12" ,
  //"com.novocode" % "junit-interface" % "0.11" % "test",
  "org.scalatest" %% "scalatest" % "3.0.1",
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
  //"com.holdenkarau" %% "spark-testing-base" % "1.6.1_0.3.2" 
  
  
)
