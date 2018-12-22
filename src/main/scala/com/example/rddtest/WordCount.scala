package com.example.rddtest

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]) {
    /**
      * VM Options : -Dspark.master=local
      */
    val conf = new SparkConf()
    conf.set("spark.app.name", "WordCount")
    conf.set("spark.master", "local")
    //conf.set("spark.ui.port", "36000")
    val sc = new SparkContext(conf)
    val wordCounts = sc.textFile("/Users/liuzhiyang/excise/wordcount.txt")
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    wordCounts.collect()
    wordCounts.foreach(println)
    //wordCounts.saveAsTextFile("/Users/liuzhiyang/excise/aaa.txt")

    //wordCount第二种方法
    val wordCounts2 = sc.textFile("/Users/liuzhiyang/excise/wordcount.txt")
      .flatMap(line => line.split(" "))
      .countByValue()
    println(wordCounts2)
  }
}
