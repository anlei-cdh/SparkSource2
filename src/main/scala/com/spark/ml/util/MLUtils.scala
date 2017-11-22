package com.spark.ml.util

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.DataFrame

object MLUtils {

  def idfFeatures(df: DataFrame, numFeatures: Int): DataFrame = {
    /**
      * 分词
      */
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val wordsData = tokenizer.transform(df)

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
