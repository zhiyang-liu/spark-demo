package com.example.stream

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Demo2 {
  def main(args: Array[String]): Unit = {
    /**
      * 有状态的转化操作
      */
    val conf = new SparkConf()
      .setAppName("SparkStreamingExample2")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)
    //局部应用设置日志级别，全局日志可以配置在log4j配置文件中
    sc.setLogLevel("WARN")
    //设置单个批次为10s
    val ssc = new StreamingContext(sc, Seconds(10))
    ssc.checkpoint("/Users/liuzhiyang/excise/checkpoint")
    val lines = ssc.socketTextStream("localhost", 7777)

    //每2个批次对前3个批次进行一次计算,每隔20s统计前30s的
    //val lines3 = lines.window(Seconds(30), Seconds(20))
    //val windowCount = lines3.count()
    //windowCount.print()

    /**
      * 让spark增量计算归约结果（并不是累计求和,还是滑动窗口内的计算，通过对移除窗口进行相反的操作）
      * 针对key-value结构需要定义移除离开窗口的操作(逆函数)：你函数的作用就是提高执行效率，并不会对结果又印象
      */
    /*val lineMap = lines.map(line => (line, 1))
    val countSum = lineMap.reduceByKeyAndWindow(
      {(x, y) => x + y},//加上新进入窗口的批次中的元素
      {(x, y) => x - y},//移除离开窗口的老批次中的元素
      Seconds(30),//窗口时长
      Seconds(10))//滑动步长
    countSum.print()*/

    /**
      * 计算当前窗口中的字符创连接
      */
    /*val linesSum = lines.reduceByWindow(
      {(x, y) => x + y},//加上新进入窗口的批次中的元素
      Seconds(30),//窗口时长
      Seconds(10)//滑动步长
    )
    linesSum.print()*/

    /**
      * 无限增长计数,累加求和
      */
    //updateFunc就要传入的参数，他是一个函数。Seq[V]表示当前key对应的所有值，Option[S] 是当前key的历史状态，返回的是新的
    def updateRunningSum(values: Seq[Long], state: Option[Long]) = {
      Some(state.getOrElse(0L) + values.size)
    }
    val lineMap = lines.map(line => (line, 1L))
    val lineMapCount = lineMap.updateStateByKey(updateRunningSum)
    lineMapCount.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
