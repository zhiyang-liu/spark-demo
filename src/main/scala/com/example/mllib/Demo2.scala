package com.example.mllib

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext

object Demo2 {
  def main(args: Array[String]): Unit = {
    /**
      * 向量基本操作
      */
    //稠密向量
    val denseVec1 = Vectors.dense(1.0, 2.0, 3.0)
    val denseVec2 = Vectors.dense(Array(1.0, 2.0, 3.0))
    //稀疏向量(维数，非零索引位置，非零元素)
    val sparseVec1 = Vectors.sparse(4, Array(0, 2), Array(1.0, 2.0))

    /**
      * TF-IDF(衡量一个关键字在文章中的重要程度)
      */
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("LogisticRegression")
    val sc = new SparkContext(conf)
    val spam = sc.textFile("./files/spam.txt")
    val tf = new HashingTF(numFeatures = 10000)
    val tfVectors = spam.map(email => tf.transform(email.split(" ")))
    val idf = new IDF()
    val idfModel = idf.fit(tfVectors)
    val tfidfVectors = idfModel.transform(tfVectors)
    tfidfVectors.foreach(println)

    /**
      * 缩放向量
      */
    val vectors = Array(Vectors.dense(Array(-2.0, 5.0, 1.0)),
      Vectors.dense(Array(2.0, 0.0, 1.0)))
    val dataset = sc.parallelize(vectors)
    val scaler = new StandardScaler(withMean = true, withStd = true)
    val model = scaler.fit(dataset)
    val result = model.transform(dataset)
    result.foreach(println)

    /**
      * 正规化(把向量正规化为长度为1)
      */
    val norVectors = new Normalizer().transform(Vectors.dense(Array(-2.0, 5.0, 1.0)))
    println(norVectors)

    /**
      * Word2Vec   (ml包用于DF，mllib包用于rdd)
      */
    val sqlContext = new SQLContext(sc)
    // Input data: Each row is a bag of words from a sentence or document.
    val documentDF = sqlContext.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")
    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model1 = word2Vec.fit(documentDF)
    val result1 = model1.transform(documentDF)
    result1.select("result").take(10).foreach(println)//每一个文章内容生成一个向量，结果为3个向量
    val vecs = model1.getVectors
    vecs.foreach(line => println(line))//每个词生成的向量结果

  }
}
