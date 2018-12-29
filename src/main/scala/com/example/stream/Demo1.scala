package com.example.stream

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Demo1 {
  def main(args: Array[String]): Unit = {
    /**
      * 先在terminal下运行：nc -l 7777   然后启动项目，在终端输入字符串即可
      */
    val conf = new SparkConf()
      .setAppName("SparkStreamingExample")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)
    //局部应用设置日志级别，全局日志可以配置在log4j配置文件中
    //sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(5))//也可以直接注入conf
    val lines = ssc.socketTextStream("localhost", 7777)
    val errorLines = lines.filter(_.contains("error"))
    errorLines.print()//DStream支持输出操作
    //test git dev
    //流式筛选
    ssc.start()
    ssc.awaitTermination()
  }
}
