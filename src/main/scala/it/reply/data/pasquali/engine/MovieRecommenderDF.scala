package it.reply.data.pasquali.engine

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

case class MovieRecommenderDF() {

  var ratings : DataFrame = null
  var als : ALS = null
  var model : ALSModel = null
  var training : DataFrame = null
  var test : DataFrame = null

  var ratingCol = ""

  def init(userCol : String, productCol : String, ratingCol : String,
           maxIter : Int, regularizationParam : Float) : MovieRecommenderDF = {

    this.ratingCol = ratingCol

    als = new ALS()
      .setMaxIter(maxIter)
      .setRegParam(regularizationParam)
      .setUserCol(userCol)
      .setItemCol(productCol)
      .setRatingCol(ratingCol)

    this
  }

  def setRatingsDataset(rates : DataFrame,
                        trainPerc : Double,
                        testPerc : Double) : MovieRecommenderDF = {

    ratings = rates
    val Array(t1, t2) = ratings.randomSplit(Array(trainPerc, testPerc))
    training = t1
    test = t2

    this
  }

  def trainModel() : MovieRecommenderDF = {
    model = als.fit(training)
    this
  }

  def evaluateModelRMSE(coldStartStrategy : String) : Double = {

    if(coldStartStrategy == null)
      model.setColdStartStrategy(MovieRecommenderDF.COLD_DROP)
    else
      model.setColdStartStrategy(coldStartStrategy)

    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol(ratingCol)
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)

    rmse
  }

  def getRecommendForAllUsers(numberOfBestItems : Int) : DataFrame = {
    model.recommendForAllUsers(numberOfBestItems)
  }


  object MovieRecommenderDF {
    val COLD_DROP = "drop"
    val COLD_NAN = "nan"
  }

}
