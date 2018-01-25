
import java.io.File

import com.typesafe.config._
import io.prometheus.client.{CollectorRegistry, Gauge}
import io.prometheus.client.exporter.PushGateway
import it.reply.data.pasquali.engine.MovieRecommender
import it.reply.data.pasquali.storage.Storage
import org.apache.spark.mllib.recommendation.Rating
import org.scalatest._

import scala.reflect.io.Path

class MRSpec
  extends FlatSpec
    with Matchers
    with OptionValues
    with Inside
    with Inspectors
    with BeforeAndAfterAll{

  var mr : MovieRecommender = null
  var config : Config = null

  var CONF_DIR = ""
  var CONFIG_FILE = "BatchML_staging.conf"

  var gaugeDuration : Gauge = null
  var pushGateway : PushGateway = null
  var JOB_NAME = ""
  var ENV = ""
  val registry = new CollectorRegistry

  override def beforeAll(): Unit = {
    super.beforeAll()
    import org.apache.hadoop.security.UserGroupInformation
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("root"))

    CONF_DIR = "conf"
    config = ConfigFactory.parseFile(new File(s"${CONF_DIR}/${CONFIG_FILE}"))
    println(config)

    //****************************************************************************

    ENV = config.getString("bml.metrics.environment")
    JOB_NAME = config.getString("bml.metrics.job_name")

    val GATEWAY_ADDR = config.getString("bml.metrics.gateway.address")
    val GATEWAY_PORT = config.getString("bml.metrics.gateway.port")

    val LABEL_PROCESS_DURATION = s"${config.getString("bml.metrics.labels.process_duration")}"

    pushGateway = new PushGateway(s"$GATEWAY_ADDR:$GATEWAY_PORT")

    // *******************************************************************************

    gaugeDuration = Gauge.build().name(LABEL_PROCESS_DURATION)
      .help("Duration of Batch ML process").register(registry)


    //****************************************************************************

  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  "The movie recommender" must "be instantiated with given parameters" in {


    val SPARK_APPNAME = config.getString("bml.spark.app_name")
    val SPARK_MASTER = config.getString("bml.spark.master")

    mr = MovieRecommender().initSpark(SPARK_APPNAME, SPARK_MASTER)

    assert(mr.spark != null)
    assert(mr.spark.sparkContext.master == "local[*]")
    assert(mr.sc != null)

  }

  it must "compute a valid model given input ratings" in {

    val timer = gaugeDuration.startTimer()

    val rawTrain = mr.sc.parallelize(Seq(

      (1 ,1, 1, 5, "time"),
      (2 ,1, 2, 5, "time"),

      (3 ,2, 1, 5, "time"),
      (4 ,2, 3, 5, "time"),
      (5 ,2, 4, 5, "time"),
      (6 ,2, 5, 0, "time"),
      (7 ,2, 6, 5, "time"),

      (8 ,4, 1, 5, "time"),
      (9 ,4, 2, 5, "time"),
      (10 ,4, 4, 5, "time"),
      (11 ,4, 5, 0, "time"),
      (12 ,4, 6, 5, "time"),

      (13 ,5, 1, 5, "time"),
      (14 ,5, 2, 5, "time"),
      (15 ,5, 4, 5, "time"),
      (16 ,5, 5, 0, "time"),
      (17 ,5, 6, 5, "time"),

      (18 ,6, 1, 5, "time"),
      (19 ,6, 3, 5, "time"),
      (20 ,6, 4, 5, "time"),
      (21 ,6, 5, 0, "time")
    ))

    val trainSet = rawTrain.map{ case (id, userID, movieID, rating, time) =>
      Rating(userID, movieID, rating.toDouble)}

    mr.trainModel(trainSet, 10, 10, 0.1)

    assert(mr.model != null)

    timer.setDuration()

    try{
      pushGateway.push(registry, s"${ENV}_${JOB_NAME}")
    }catch{
      case _ : Exception => println("Unable to reach Metric Pushgateway")
    }
  }

  "The computed model" should
    "estimate a new entries with good precision" in {

    val rawTest = mr.sc.parallelize(Seq(

      (1, 5, 0, "time"),
      (1, 6, 5, "time")
    ))

    val testSet = rawTest.map{ case (userID, movieID, rating, time) =>
      Rating(userID, movieID, rating.toDouble)}

    val mse = mr.evaluateModel_MSE(testSet)

    assert(mse < 0.01)
  }

  it should "can be saved in zip format and retrieved" in {

    val MODEL_PATH = config.getString("bml.recommender.model_path")
    val MODEL_ARCHIVE_PATH = config.getString("bml.recommender.model_archive_path")

    println(s"MODEL PATH --->>> ${MODEL_PATH}")
    println(s"MODEL ARCHIVE PATH --->>> ${MODEL_ARCHIVE_PATH}")

    if(new File(MODEL_PATH).exists())
    {
      Path(MODEL_PATH).deleteRecursively()
      println("[WARN] old model deleted")
    }

    if(new File(MODEL_ARCHIVE_PATH).exists())
    {
      Path(MODEL_ARCHIVE_PATH).delete()
      println("[WARN] old archive deleted")
    }

    mr.storeModel(MODEL_PATH)
    assert(new File(MODEL_PATH).exists)
    mr.model = null

    val storage = Storage()
    storage.zipModel(MODEL_PATH, MODEL_ARCHIVE_PATH)
    assert(new File(MODEL_ARCHIVE_PATH).exists)

    Path(MODEL_PATH).deleteRecursively()
    println("[WARN] old model deleted")

    storage.unzipModel(MODEL_ARCHIVE_PATH, MODEL_PATH)
    mr.loadModel(MODEL_PATH)
    assert(mr.model != null)

    Path(MODEL_PATH).deleteRecursively()
    println("[WARN] model deleted, archive mantained")
  }





}
