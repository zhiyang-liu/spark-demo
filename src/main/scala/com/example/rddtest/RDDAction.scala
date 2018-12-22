package com.example.rddtest

import org.apache.spark.{SparkConf, SparkContext}

object RDDAction {
  def main(args: Array[String]): Unit = {
    /**
      * 常用的RDD的action操作（通用）：collect,count,countByValue,take,top,reduce,fold,aggregate,foreach
      */
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("WordCount")
    val sc = new SparkContext(conf)

    val nums = sc.parallelize(List(1, 2, 3, 4))
    val sum = nums.reduce((x, y) => x + y)//操作两个相同类型的rdd数据，并返回一个同类型的新元素
    val sumAndZero = nums.fold(10)((x, y) => x + y)//fold与reduce类似，加上一个初始值作为每个分区第一次调用时的结果
    println(sumAndZero)

    //将rdd加入缓存然后手动从缓冲中移除，需要缓存的根本原因是spark的RDD是惰性求值的
    nums.persist()
    nums.unpersist()

    //求平均值的两种方法
    val numsAvg1 = nums.map(x => (x, 1)).fold((0, 0))((acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    val numsAvg2 = nums.aggregate((0, 0))((acc, value) => (acc._1 + value, acc._2 + 1),//acc是map类型表示新的元素mao类型,value为rdd中每一个元素值类型
        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    println(numsAvg2._1 / numsAvg2._2)

  }
}
