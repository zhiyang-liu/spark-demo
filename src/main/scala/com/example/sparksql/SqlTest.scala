package com.example.sparksql

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SqlTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("sql1")
    val sc = new SparkContext(conf)
    val hiveCtx = new HiveContext(sc)
    val input = hiveCtx.jsonFile("./files/test.json")
    input.printSchema()//输出SchemaRDD的结构信息
    input.registerTempTable("persons")
    val topTweets = hiveCtx.sql("SELECT id, name From persons")
    val mothers = hiveCtx.sql("SELECT family.mother from persons")//访问嵌套字段，如果数组，就：columnName[0]
    topTweets.foreach(
      line =>
        println(line.getAs[String]("id") + " " + line.getAs[String]("name"))
    )
    mothers.foreach(
      line =>
        println(line.getAs[String]("mother"))
    )

    /**
      * 将普通RDD装换DataFrame,如果是SchemaRDD，RDD。registerTempTable("persons")
      */
    val happyPersonRDD = sc.parallelize(List(HappyPerson("holden", "coffee"), HappyPerson("holdehn1", "coffee1")))
    val sparkSession = SparkSession.builder()
      .config(conf).getOrCreate()
    val happyPersonDF = sparkSession.createDataFrame(happyPersonRDD)
    happyPersonDF.printSchema()
    happyPersonDF.registerTempTable("happyPersons")
    val handles = hiveCtx.sql("SELECT handle from happyPersons")
    handles.foreach(
      line =>
        println(line.getAs[String]("handle"))
    )

    /**
      * 自定义函数 UDF
      */
    hiveCtx.udf.register("strlenScala", (_:String).length)
    val len = hiveCtx.sql("SELECT strlenScala(handle) from happyPersons where favouriteBeverage = 'coffee'")
    len.foreach(
      line =>
        println(line.getAs(0))
    )
  }
}


