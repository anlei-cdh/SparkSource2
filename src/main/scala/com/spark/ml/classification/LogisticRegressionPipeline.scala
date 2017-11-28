package com.spark.ml.classification

import com.spark.ml.util.TrainingUtils
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.SparkSession

object LogisticRegressionPipeline {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName(s"${this.getClass.getSimpleName}").getOrCreate()

    val path = "model/lr"
    val numFeatures = 10000

    /**
      * 训练集
      */
    val trainingDataFrame = spark.createDataFrame(TrainingUtils.trainingData).toDF("id", "text","label")
    /**
      * 分词
      */
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    /**
      * 向量化
      */
    val hashingTF = new HashingTF().setInputCol(tokenizer.getOutputCol).setOutputCol("features").setNumFeatures(numFeatures)
    /**
      * TF-IDF 管道使用这个特征不准确
      */
    // val idf = new IDF().setInputCol(hashingTF.getOutputCol).setOutputCol("features")
    /**
      * 逻辑回归
      */
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.001)
    /**
      * 管道 - 分词,向量化,逻辑回归
      */
    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))
    /**
      * 保存模型 - 管道方式
      */
    pipeline.fit(trainingDataFrame).write.overwrite().save(path)

    /**
      * 测试集
      */
    val testDataFrame = spark.createDataFrame(TrainingUtils.testData).toDF("id", "text")
    /**
      * 读取模型 - 管道方式
      */
    val pipelineModel = PipelineModel.load(path)
    /**
      * 分类结果 - 管道方式
      */
    val result = pipelineModel.transform(testDataFrame)

    result.show(false)

    spark.stop()
  }

}
