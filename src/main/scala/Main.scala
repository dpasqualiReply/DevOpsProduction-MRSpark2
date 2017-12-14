
import it.reply.pasquali.engine.MovieRecommender
import it.reply.pasquali.storage.Storage
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.log4j.Logger

import scala.reflect.io.Path
import scala.util.Try


object Main {

    def main(args: Array[String]): Unit = {

        val log = Logger.getLogger(getClass.getName)

        log.info("***** Init Spark Session *****")

        val spark = SparkSession.builder()
          .appName("batch machine learning")
          .master("local[*]")
          .getOrCreate()

        log.info("***** Take Spark Context from Spark Session *****")

        val sc = spark.sparkContext

        log.info("***** Init Storage connector *****")

        val storage = Storage()
        storage.init()

        log.info("***** Init Kudu Datamart Connection *****")

        storage.initKudu("cloudera-vm.c.endless-upgrade-187216.internal", "7051")

        log.info("***** Read ratings table as RDD *****")

        val ratings = storage.readKuduTable("datamart.ratings").rdd

        log.info("***** Split it in train set and test set *****")

        val Array(rawTrain, rawTest) = ratings.randomSplit(Array(0.8, 0.2))

        log.info("***** Remap to Rating(user, movie, rate) *****")

        val testSet = rawTest.map{ case Row(userID, movieID, rating, time) =>
            Rating(userID.asInstanceOf[Long].toInt,
                movieID.asInstanceOf[Long].toInt,
                rating.asInstanceOf[Double])}

        val trainSet = rawTrain.map{ case Row(userID, movieID, rating, time) =>
            Rating(userID.asInstanceOf[Long].toInt,
                movieID.asInstanceOf[Long].toInt,
                rating.asInstanceOf[Double])}

        log.info("***** Estimate model with ALS *****")

        val mr = MovieRecommender()
          .initSpark(spark)
          .trainModel(trainSet, 10, 10, 0.1)

        log.info("***** Evaluate Model *****")

        val mse = mr.evaluateModel_MSE(testSet)

        log.info(s"***** Actual MSE is ${mse} *****")
        println(s"Actual MSE is ${mse}")

        log.info("***** Store model *****")

        mr.storeModel("out/m20Model")

        log.info("***** Zip it to deploy *****")

        storage.zipModel("out/m20Model", "out/m20Model.zip")

        log.info("***** Drop model folder *****")

        Try(Path("out/m20Model").deleteRecursively())

        log.info("***** Close Spark Session *****")

        spark.stop()
    }
}
