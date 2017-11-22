package com.spark.ml.util

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.DataFrame

object MLUtils {

  def hashingFeatures(dataFrame: DataFrame, numFeatures: Int): DataFrame = {
    hashingFeatures(dataFrame, numFeatures, "features")
  }

  def hashingFeatures(dataFrame: DataFrame, numFeatures: Int,outputCol: String): DataFrame = {
    /**
      * 分词
      */
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val wordsData = tokenizer.transform(dataFrame)

    /**
      * 向量化
      */
    val hashingTF = new HashingTF().setInputCol(tokenizer.getOutputCol).setOutputCol(outputCol).setNumFeatures(numFeatures)
    val featurizedData = hashingTF.transform(wordsData)

    featurizedData.show(false)

    featurizedData
  }

  def idfFeatures(dataFrame: DataFrame, numFeatures: Int): DataFrame = {
    /**
      * 分词
      */
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val wordsData = tokenizer.transform(dataFrame)

    /**
      * 向量化
      */
    val hashingTF = new HashingTF().setInputCol(tokenizer.getOutputCol).setOutputCol("rawFeatures").setNumFeatures(numFeatures)
    val featurizedData = hashingTF.transform(wordsData)

    /**
      * TF-IDF
      */
    val idf = new IDF().setInputCol(hashingTF.getOutputCol).setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)

    rescaledData.show(false)

    rescaledData
  }

}
