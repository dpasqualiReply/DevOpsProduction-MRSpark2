
import java.io.File

import com.typesafe.config.ConfigFactory
import it.reply.data.pasquali.Storage
import it.reply.data.pasquali.engine.MovieRecommender
import org.apache.spark.mllib.recommendation.Rating
import org.scalatest._

import sys.process._

class MRSpec
  extends FlatSpec
    with Matchers
    with OptionValues
    with Inside
    with Inspectors
    with BeforeAndAfterAll{

  var mr : MovieRecommender = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    import org.apache.hadoop.security.UserGroupInformation
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("root"))
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  "The movie recommender" must "be instantiated with given parameters" in {


    val config = ConfigFactory.parseFile(new File("/opt/conf/BatchML_staging.conf"))
    mr = MovieRecommender().initSpark("mr rdd test", "local[*]")

    assert(mr.spark != null)
    assert(mr.spark.sparkContext.master == "local[*]")
    assert(mr.sc != null)

  }

  it must "compute a valid model given input ratings" in {

    val rawTrain = mr.sc.parallelize(Seq(

      (1, 1, 5, "time"),
      (1, 2, 5, "time"),

      (2, 1, 5, "time"),
      (2, 3, 5, "time"),
      (2, 4, 5, "time"),
      (2, 5, 0, "time"),
      (2, 6, 5, "time"),

      (4, 1, 5, "time"),
      (4, 2, 5, "time"),
      (4, 4, 5, "time"),
      (4, 5, 0, "time"),
      (4, 6, 5, "time"),

      (5, 1, 5, "time"),
      (5, 2, 5, "time"),
      (5, 4, 5, "time"),
      (5, 5, 0, "time"),
      (5, 6, 5, "time"),

      (6, 1, 5, "time"),
      (6, 3, 5, "time"),
      (6, 4, 5, "time"),
      (6, 5, 0, "time")
    ))

    val trainSet = rawTrain.map{ case (userID, movieID, rating, time) =>
      Rating(userID, movieID, rating.toDouble)}

    mr.trainModel(trainSet, 10, 10, 0.1)

    assert(mr.model != null)
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

    mr.storeModel("testModel")
    assert(new File("testModel").exists)
    mr.model = null

    val storage = Storage()
    storage.zipModel("testModel", "testModel.zip")
    assert(new File("testModel.zip").exists)

    storage.unzipModel("testModel.zip", "testModelUz")
    mr.loadModel("testModelUz")
    assert(mr.model != null)
  }





}
