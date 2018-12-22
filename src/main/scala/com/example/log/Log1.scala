package com.example.log

import org.apache.spark.{SparkConf, SparkContext}

object Log1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("WordCount")
    val sc = new SparkContext(conf)
    val input = sc.textFile("./files/input.txt")
    println(input.toDebugString)
    val tokenized = input
      .map(line => line.split(" "))
      .filter(words => words.size > 0)
    val counts = tokenized
      .map(words => (words(0), 1))
      .reduceByKey((a, b) => a + b)
    println(counts.toDebugString)
    counts.foreach(println)
  }
}
