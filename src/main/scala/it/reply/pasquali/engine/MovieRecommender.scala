package it.reply.pasquali.engine

import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

case class MovieRecommender() {

  var spark : SparkSession = null
  var sc : SparkContext = null

  var model : MatrixFactorizationModel = null

  def initSpark(spark : SparkSession) : MovieRecommender = {

    this.spark = spark
    sc = spark.sparkContext
    this
  }

  def initSpark(appName : String, master : String) : MovieRecommender = {

    spark = SparkSession.builder().appName(appName).master(master).getOrCreate()
    sc = spark.sparkContext
    this
  }

  def trainModel(ratings : RDD[Rating], rank : Int, numIter : Int, regParam : Double) : MovieRecommender = {
    model = ALS.train(ratings, rank, numIter, regParam)
    this
  }

  def evaluateModel_MSE(testSet : RDD[Rating]) : Double = {

    val usersProducts = testSet.map { case Rating(user, product, rate) =>
      (user, product)
    }

    val predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }

    val ratesAndPreds = testSet.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)

    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()

    MSE
  }

  def predict(ratings : RDD[(Int, Int)]) : RDD[((Int, Int), Double)] = {

    model.predict(ratings).map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }
  }

  def storeModel(dirPath : String) : Unit = {

    model.save(sc, dirPath)
  }

  def loadModel(dirPath : String) : MovieRecommender = {

    model = MatrixFactorizationModel.load(sc, dirPath)
    this
  }

  def closeSession() : Unit = {

    if(spark != null)
      spark.stop()

    spark = null
    sc = null

  }

}
