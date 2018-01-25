
import java.io.File

import com.typesafe.config.ConfigFactory
import io.prometheus.client.{CollectorRegistry, Gauge}
import io.prometheus.client.exporter.PushGateway
import it.reply.data.pasquali.engine.MovieRecommender
import it.reply.data.pasquali.storage.Storage
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.log4j.Logger

import scala.reflect.io.Path
import scala.util.Try


object Main {

  def main(args: Array[String]): Unit = {

    //CONF_DIR = scala.util.Properties.envOrElse("DEVOPS_CONF_DIR", "conf")
    val CONF_DIR = "conf"

    //val configuration = ConfigFactory.parseFile(new File(s"${CONF_DIR}/${CONFIG_FILE}"))
    val configuration = ConfigFactory.load()

    val SPARK_APPNAME = configuration.getString("bml.spark.app_name")
    val SPARK_MASTER = configuration.getString("bml.spark.master")

    val MODEL_PATH = configuration.getString("bml.recommender.model_path")
    val MODEL_ARCHIVE_PATH = configuration.getString("bml.recommender.model_archive_path")
    val TEST_FRACTION = configuration.getDouble("bml.recommender.test_fraction")
    val TRAIN_FRACTION = configuration.getDouble("bml.recommender.train_fraction")

    val KUDU_ADDRESS = configuration.getString("bml.kudu.address")
    val KUDU_PORT = configuration.getString("bml.kudu.port")
    val KUDU_RATINGS_TABLE = configuration.getString("bml.kudu.ratings_table")
    val KUDU_DATABASE = configuration.getString("bml.kudu.database")
    val KUDU_TABLE_BASE = configuration.getString("bml.kudu.table_base")

    //****************************************************************************

    val ENV = configuration.getString("bml.metrics.environment")
    val JOB_NAME = configuration.getString("bml.metrics.job_name")

    val GATEWAY_ADDR = configuration.getString("bml.metrics.gateway.address")
    val GATEWAY_PORT = configuration.getString("bml.metrics.gateway.port")

    val LABEL_RATINGS_NUMBER = s"${configuration.getString("bml.metrics.labels.ratings_number")}"
    val LABEL_USERS_NUMBER = s"${configuration.getString("bml.metrics.labels.users_number")}"
    val LABEL_MOVIES_NUMBER = s"${configuration.getString("bml.metrics.labels.movies_number")}"

    val LABEL_MODEL_SIZE = s"${configuration.getString("bml.metrics.labels.model_size")}"
    val LABEL_MSE = s"${configuration.getString("bml.metrics.labels.mse")}"

    val LABEL_PROCESS_DURATION = s"${configuration.getString("bml.metrics.labels.process_duration")}"

    val pushGateway : PushGateway = new PushGateway(s"$GATEWAY_ADDR:$GATEWAY_PORT")
    val registry = new CollectorRegistry

    // *******************************************************************************

    val gaugeRatingsNumber : Gauge = Gauge.build().name(LABEL_RATINGS_NUMBER)
      .help("\"Number of ratings in the model\"").register(registry)

    val gaugeUsersNumber : Gauge = Gauge.build().name(LABEL_USERS_NUMBER)
      .help("Number of users in the model").register(registry)

    val gaugeMoviesNumber : Gauge = Gauge.build().name(LABEL_MOVIES_NUMBER)
      .help("Number of movies in the model").register(registry)

    val gaugeModelSize : Gauge = Gauge.build().name(LABEL_MODEL_SIZE)
      .help("Size of the zipped model").register(registry)

    val gaugeMSE : Gauge = Gauge.build().name(LABEL_MSE)
      .help("Minimum Squared Error of the model").register(registry)

    val gaugeDuration : Gauge = Gauge.build().name(LABEL_PROCESS_DURATION)
      .help("Duration of Batch ML process").register(registry)


    //****************************************************************************

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

    storage.initKudu(KUDU_ADDRESS, KUDU_PORT, KUDU_TABLE_BASE)

    val timer = gaugeDuration.startTimer()

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

    timer.setDuration()

    gaugeRatingsNumber.set(ratings.count())
    gaugeMoviesNumber.set(mr.model.productFeatures.count())
    gaugeUsersNumber.set(mr.model.userFeatures.count())
    gaugeMSE.set(mse)
    gaugeModelSize.set(new File(MODEL_ARCHIVE_PATH).length)

    pushGateway.push(registry, s"${ENV}_${JOB_NAME}")

    log.info("***** Close Spark Session *****")

    spark.stop()

    println("BATCH ML PROCESS DONE")
  }
}
