
import it.reply.pasquali.engine.MovieRecommender
import it.reply.pasquali.storage.Storage
import org.apache.spark.mllib.recommendation.Rating
import org.scalatest._

import scala.reflect.io.File

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
  }

  override def afterAll(): Unit = {
    super.afterAll()

  }

  "The movie recommender" must "be instantiated with given parameters" in {

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

    mr.storeModel("m20Model")
    assert(File("m20Model").exists)
    mr.model = null

    val storage = Storage()
    storage.zipModel("m20Model", "m20Model.zip")
    assert(File("m20Model.zip").exists)

    storage.unzipModel("m20Model.zip", "m20ModelUnzipped")
    mr.loadModel("m20ModelUnzipped")
    assert(mr.model != null)
  }





}
