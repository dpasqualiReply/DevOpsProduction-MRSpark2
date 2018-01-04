import org.scalatest._


class StorageSpec
  extends FlatSpec
    with Matchers
    with OptionValues
    with Inside
    with Inspectors
    with BeforeAndAfterAll{

//  var storage : Storage = null
//
//  override def beforeAll(): Unit = {
//    super.beforeAll()
//    storage = Storage()
//    storage.init()
//  }
//
//  override def afterAll(): Unit = {
//    super.afterAll()
//    storage.closeSession()
//  }

  val kuduTestTable = "default.testKudu"

  "The Storage" must "initialize Spark Session and Kudu Context" in {

    pending

//    storage.initKudu("cloudera-vm.c.endless-upgrade-187216.internal", "7051")
//
//    assert(storage.kuduMaster != null)
//    assert(storage.spark != null)
//    assert(storage.kuduContext.kuduMaster == "cloudera-vm.c.endless-upgrade-187216.internal:7051")
  }

  it must "connect to test table default.testTable, insert a row and retrieve it" in {

    pending
//    val test = storage.spark.sparkContext.parallelize(Seq((99, "test")))
//    val testDF = storage.spark.createDataFrame(test).toDF("id", "value")
//
//    storage.insertKuduRows(testDF, kuduTestTable)
//
//    val table = storage.readKuduTable(kuduTestTable)
//
//    assert(table.where("id == 99 && value == 'test'").count() == 1)
  }

  it should "can update a row" in {

    pending

//    val test = storage.spark.sparkContext.parallelize(Seq((99, "testUpdate")))
//    val testDF = storage.spark.createDataFrame(test).toDF("id", "value")
//
//    storage.updateKuduRows(testDF, kuduTestTable)
//
//    val table = storage.readKuduTable(kuduTestTable)
//
//    assert(table.where("id == 99 && value == 'testUpdate'").count() == 1)
  }

  it should "can delete a row" in {

    pending
//
//    val test = storage.spark.sparkContext.parallelize(Seq((99, "testUpdate")))
//    val testDF = storage.spark.createDataFrame(test).toDF("id", "value")
//
//    val keys = testDF.select("id")
//
//    storage.deleteKuduRows(keys, kuduTestTable)
//
//    val table = storage.readKuduTable(kuduTestTable)
//
//    assert(table.where("id == 99").count() == 0)
  }

}
