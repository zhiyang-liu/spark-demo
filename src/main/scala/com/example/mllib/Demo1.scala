package com.example.mllib

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

object Demo1 {
  def main(args: Array[String]): Unit = {
    /**
      * 垃圾邮件分类,利用logistic回归
      */
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("LogisticRegression")
    val sc = new SparkContext(conf)
    val spam = sc.textFile("./files/spam.txt")
    val normal = sc.textFile("./files/normal.txt")
    //创建一个HashingTF实例来把邮件文本映射为包含10000个特征的向量
    val tf = new HashingTF(numFeatures = 10000)
    //各邮件被切分为单词，每个单词被映射为一个特征
    val spamFeatrues = spam.map(email => tf.transform(email.split(" ")))
    val normalFeatures = normal.map(email => tf.transform(email.split(" ")))
    /**
      *spamFeatrues是稀疏向量，分别代表（维数，单词的哈希值(不为0的位置)，单词的频数）
      */
    //normalFeatures.foreach(println)
    //创建LabelPoint数据集分别存放垃圾邮件和正常邮件的例子
    val positiveExamples = spamFeatrues.map(features => LabeledPoint(1, features))
    val negativeExamples = normalFeatures.map(features => LabeledPoint(0, features))
    val trainingData = positiveExamples.union(negativeExamples)
    trainingData.cache()//因为逻辑回归是迭代算法，所以缓存训练数据
    //使用SGD算法进行逻辑回归
    val model = new LogisticRegressionWithSGD().run(trainingData)
    //以垃圾邮件及正常邮件的例子分别进行测试
    val posTest = tf.transform(
      "O M G GET cheap stuff by sending money to ...".split(" "))
    val negTest = tf.transform(
      "Hi Dad, I started studying Spark the other ...".split(" "))
    println("Prediction for positive test example:" + model.predict(posTest))
    println("Prediction for negative test example:" + model.predict(negTest))
  }
}
