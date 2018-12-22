name := "scala-sbt"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.1.1", //% "provided" //打包到集群运行可以添加provided

  //ChapterSixExample用到的jar包
  "org.eclipse.jetty"  % "jetty-client" % "8.1.14.v20131031",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.3.3",
  //"com.fasterxml.jackson.module" % "jackson-module-scala_2.10" % "2.3.3",   //该包打包错误可以尝试注释该引用
  "org.apache.spark" % "spark-hive_2.11" % "2.1.1",
  "org.apache.spark" % "spark-streaming_2.11" % "2.1.1",
  "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.1.1",//引入kafka,只使用这一个就可以
  //"org.apache.spark" % "spark-streaming-kafka_2.11" % "1.4.0",//用到了logging，spark2已经移除，所以用上面的kafka包替换
  "org.apache.spark" % "spark-mllib_2.11" % "2.1.1",
  "log4j" % "log4j" % "1.1.3"

)