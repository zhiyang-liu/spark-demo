package com.example.mllib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}

object Demo5 {

  case class Document(id: Long, text: String)

  case class LabeledDocument(id: Long, text: String, label: Double)

  def main(args: Array[String]) {
    /**
      * 使用流水线api实现垃圾邮件分类
      */
    val conf = new SparkConf()
      .setAppName("BookExamplePipeline")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder()
      .config(conf).getOrCreate()
    import sparkSession.implicits._
    // Load 2 types of emails from text files: spam and ham (non-spam).
    // Each line has text from one email.
    val spam = sc.textFile("./files/spam.txt")
    val ham = sc.textFile("./files/normal.txt")

    // Create LabeledPoint datasets for positive (spam) and negative (ham) examples.
    val positiveExamples = spam.zipWithIndex().map { case (email, index) =>
      LabeledDocument(index, email, 1.0)
    }
    val negativeExamples = ham.zipWithIndex().map { case (email, index) =>
      LabeledDocument(index, email, 0.0)
    }
    val trainingData = positiveExamples ++ negativeExamples

    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    // Each stage outputs a column in a SchemaRDD and feeds it to the next stage's input column.
    val tokenizer = new Tokenizer() // Splits each email into words
      .setInputCol("text")
      .setOutputCol("words")
    val hashingTF = new HashingTF() // Maps email words to vectors of 100 features.
      .setNumFeatures(100)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val lr = new LogisticRegression() // LogisticRegression uses inputCol "features" by default.
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))

    // Fit the pipeline to training documents.
    // RDDs of case classes work well with Pipelines since Spark SQL can infer a schema from
    // case classes and convert the data into a SchemaRDD.
    val model = pipeline.fit(trainingData.toDF)

    // Make predictions on test documents.
    // The fitted model automatically transforms features using Tokenizer and HashingTF.
    val testData = sc.parallelize(Seq(
      Document(0, "O M G GET cheap stuff by sending money to ..."), // positive example (spam)
      Document(1, "Hi Dad, I started studying Spark the other ...")   // negative example (ham)
    ))
    val predictions = model.transform(testData.toDF)
      .select('id, 'prediction).collect()
      .map { case Row(id, prediction) => (id, prediction) }.toMap
    println(s"Prediction for positive test example: ${predictions(0)}")
    println(s"Prediction for negative test example: ${predictions(1)}")
  }
}

