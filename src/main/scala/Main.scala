
import java.io.File

import com.typesafe.config.ConfigFactory
import it.reply.data.pasquali.Storage
import it.reply.data.pasquali.engine.MovieRecommender
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.log4j.Logger

import scala.reflect.io.Path
import scala.util.Try


object Main {

  var SPARK_APPNAME = ""
  var SPARK_MASTER = ""

  var KUDU_ADDRESS = ""
  var KUDU_PORT = ""
  var KUDU_RATINGS_TABLE = ""
  var KUDU_DATABASE = ""

  var MODEL_PATH = ""
  var MODEL_ARCHIVE_PATH = ""
  var TEST_FRACTION : Double = 0.0
  var TRAIN_FRACTION : Double = 0.0


  def main(args: Array[String]): Unit = {
    //val configuration = ConfigFactory.load("BatchML")

    val configuration = ConfigFactory.parseFile(new File("/opt/conf/BatchML_staging.conf"))

    SPARK_APPNAME = configuration.getString("bml.spark.app_name")
    SPARK_MASTER = configuration.getString("bml.spark.master")

    MODEL_PATH = configuration.getString("bml.recommender.model_path")
    MODEL_ARCHIVE_PATH = configuration.getString("bml.recommender.model_archive_path")
    TEST_FRACTION = configuration.getDouble("bml.recommender.test_fraction")
    TRAIN_FRACTION = configuration.getDouble("bml.recommender.train_fraction")

    KUDU_ADDRESS = configuration.getString("bml.kudu.address")
    KUDU_PORT = configuration.getString("bml.kudu.port")
    KUDU_RATINGS_TABLE = configuration.getString("bml.kudu.ratings_table")
    KUDU_DATABASE = configuration.getString("bml.kudu.database")

    val log = Logger.getLogger(getClass.getName)

    log.info("***** Init Spark Session *****")

    val spark = SparkSession.builder()
      .appName(SPARK_APPNAME)
      .master(SPARK_MASTER)
      .getOrCreate()

    log.info("***** Take Spark Context from Spark Session *****")

    val sc = spark.sparkContext

    log.info("***** Init Storage connector *****")

    val storage = Storage()
      .init(SPARK_MASTER, SPARK_APPNAME, false)

    log.info("***** Init Kudu Datamart Connection *****")

    storage.initKudu(KUDU_ADDRESS, KUDU_PORT)

    log.info("***** Read ratings table as RDD *****")

    val ratings = storage.readKuduTable(s"${KUDU_DATABASE}.${KUDU_RATINGS_TABLE}").rdd

    log.info("***** Split it in train set and test set *****")

    val Array(rawTrain, rawTest) = ratings.randomSplit(Array(TRAIN_FRACTION, TEST_FRACTION))

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

    mr.storeModel(MODEL_PATH)

    log.info("***** Zip it to deploy *****")

    storage.zipModel(MODEL_PATH, MODEL_ARCHIVE_PATH)

    log.info("***** Drop model folder *****")

    Try(Path(MODEL_PATH).deleteRecursively())

    log.info("***** Close Spark Session *****")

    spark.stop()
  }
}
