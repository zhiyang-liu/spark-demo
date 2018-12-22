package com.example.mllib

import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithSGD}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.{SparkConf, SparkContext}

object Demo6 {
  def main(args: Array[String]): Unit = {
    /**
      * 将demo1中保存的逻辑回归的模型直接加载使用
      */
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("LogisticRegressionLoad")
    val sc = new SparkContext(conf)
    val tf = new HashingTF(numFeatures = 10000)
    //加载持久化的模型
    val model = LogisticRegressionModel.load(sc, "./files/logisticRegression.model")
    val posTest = tf.transform(
      "O M G GET cheap stuff by sending money to ...".split(" "))
    val negTest = tf.transform(
      "Hi Dad, I started studying Spark the other ...".split(" "))
    println("Prediction for positive test example:" + model.predict(posTest))
    println("Prediction for negative test example:" + model.predict(negTest))
  }
}
