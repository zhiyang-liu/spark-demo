package com.example.rddtest

import org.apache.spark.{SparkConf, SparkContext}

object PairRDD {

  def main(args: Array[String]): Unit = {
    /**
      * 常用的Pair RDD转化操作（只针对键值型）：reduceByKey,groupByKey,combineByKey,mapValues.flatMapValues,keys,values,sortByKey
      * 常用的Pair RDD行动操作（只针对键值型）：countByKey,collectAsMap,lookup(key)
      */
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("WordCount")
    val sc = new SparkContext(conf)

    val lines = sc.parallelize(List("error zz", "error warn", "warn aa bb", "error", "warn cc", "nomal"))

    //提取每行第一个单词
    val pairsFirst = lines.map(x => (x.split(" ")(0), 1))
    val firstSameSum = pairsFirst.reduceByKey((x, y) => x + y)
    val pairsFirstValue = pairsFirst.mapValues(x => x + 1)
    val firstKeys = pairsFirst.keys
    val firstValues = pairsFirst.values
    val pairsFirstByKey = pairsFirst.sortByKey()
    val firstGroupKey = pairsFirst.groupByKey()
    //firstGroupKey.foreach(println)

    //针对的是key
    val firstJoin = pairsFirst.join(pairsFirst)
    val firstSub = pairsFirst.subtract(pairsFirst)
    //firstJoin.foreach(println)

    val pairs = lines.map(x => (x.split(" ")(0), x))
    val pairs5 = pairs.filter{case(key, value) => value.length <= 5}
    //pairs5.foreach(println)

    //将键相同的值合并起来
    val reduceByKey = pairsFirst.reduceByKey((x, y) => x + y)
    //求每个键的平均值
    val mapReduceByKey = pairsFirst.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    //mapReduceByKey.foreach(println)

    /**
      * 参数含义的解释
      * a 、score => (1, score)，我们把分数作为参数,并返回了附加的元组类型。 以"Fred"为列，
      * 当前其分数为88.0 =>(1,88.0)  1表示当前科目的计数器，此时只有一个科目
      *
      * b、(c1: MVType, newScore) => (c1._1 + 1, c1._2 + newScore)，
      * 注意这里的c1就是createCombiner初始化得到的(1,88.0)。在一个分区内，我们又碰到了"Fred"的一个新的分数91.0。
      * 当然我们要把之前的科目分数和当前的分数加起来即c1._2 + newScore,然后把科目计算器加1即c1._1 + 1
      *
      * c、 (c1: MVType, c2: MVType) => (c1._1 + c2._1, c1._2 + c2._2)，
      * 注意"Fred"可能是个学霸,他选修的科目可能过多而分散在不同的分区中。
      * 所有的分区都进行mergeValue后,接下来就是对分区间进行合并了,分区间科目数和科目数相加分数和分数相加就得到了总分和总科目数
      */
    val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0))
    val d1 = sc.parallelize(initialScores)
    type MVType = (Int, Double) //定义一个元组类型(科目计数器,分数)
    d1.combineByKey(
      score => (1, score),
      (c1: MVType, newScore) => (c1._1 + 1, c1._2 + newScore),
      (c1: MVType, c2: MVType) => (c1._1 + c2._1, c1._2 + c2._2)
    ).map { case (name, (num, socre)) => (name, socre / num) }.collect.foreach(println)

    //通用RDD操作
    val fredScore = initialScores.filter(v => v._1 == "Fred")
    val initialScoresSecond = initialScores.map(v => (v._1, v._2 * 2))
    initialScoresSecond.foreach(println)
    println(d1.partitions.size)

  }

}
