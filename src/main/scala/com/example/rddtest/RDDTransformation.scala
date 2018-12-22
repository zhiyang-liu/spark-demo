package com.example.rddtest

import org.apache.spark.{SparkConf, SparkContext}

object RDDTransformation {
  def main(args: Array[String]): Unit = {
    /**
      * 常用的RDD的转化操作（通用）：map,flatmap,filter,distinct,sample,union等
      */
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("WordCount")
    val sc = new SparkContext(conf)

    val lines = sc.parallelize(List("error zz", "error warn", "warn aa bb", "error", "warn cc", "nomal"))

    val errorLine = lines.filter(line => line.contains("error"))
    val warnLines = lines.filter(line => line.contains("warn"))

    val errorAndWarnUnion = errorLine.union(warnLines)//不会去重，可以使用distinct实现去重效果
    val errorAndWarnIntersect= errorLine.intersection(warnLines)//求交集
    val errorSubWarn = errorLine.subtract(warnLines)//出现在errorLine而不出现在warnLine
    val errorCartWarn = errorLine.cartesian(warnLines)//做笛卡尔积，返回map
    errorCartWarn.foreach(println)
    //println(errorSubWarn.collect().mkString(","))
    //println(errorAndWarnIntersect.count())
    //println(errorAndWarnUnion.distinct().count())
    //println(errorAndWarn.count())


    val errorAndWarnLine = errorAndWarnUnion.map(line => (line, 1))
    errorAndWarnLine.take(3).foreach(println)

    val nums = sc.parallelize(List(1, 2, 3, 4))

    //对各个数字计算求平方，map的返回值类型不需要要和输入类型一样，因此可以转换RDD的类型
    val result = nums.map(x => x * x)
    val resultMap = nums.map(x => (x, x * x))
    println(resultMap.collect().mkString(","))

    //对每个输入元素生成多个输出元素
    val words = lines.flatMap(line => line.split(" "))
    println(words.collect().mkString(","))

    val numsSample = nums.sample(false, 0.5)//false表示无放回的抽样，fraction表示抽样比例
    println(numsSample.collect().mkString(","))
  }
}
