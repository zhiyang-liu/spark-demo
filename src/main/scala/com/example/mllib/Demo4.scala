package com.example.mllib

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Demo4 {
  def main(args: Array[String]): Unit = {
    /**
      * pca + kmeans
      */
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("PCAtest")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder()
      .config(conf).getOrCreate()
    val data = Array(
      Vectors.dense(4.0,1.0, 4.0, 5.0),
      Vectors.dense(2.0,3.0, 4.0, 5.0),
      Vectors.dense(4.0,0.0, 6.0, 7.0))
    val points  = sc.parallelize(data)
    val mat = new RowMatrix(points)
    val pc = mat.computePrincipalComponents(2)
    //将点投影到低维空间中，projected为降维后的结果
    val projected = mat.multiply(pc).rows
    projected.foreach(println)

    /**
      * 训练kmeans，分成两个类别
      */
    val model = KMeans.train(projected, 2, 10, 10)
    println(model.k)
    val result = model.predict(projected)
    result.foreach(println)//输出每条记录所属的类别

  }
}
