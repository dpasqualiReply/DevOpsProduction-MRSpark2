package it.reply.pasquali.storage

import java.io.File
import sys.process._

import org.apache.kudu.spark.kudu._
import org.apache.spark.sql._
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.conf.Configuration
import org.zeroturnaround.zip.ZipUtil

case class Storage() {

  var kuduMaster : String = "cloudera-vm.c.endless-upgrade-187216.internal"
  var kuduPort : String = "7051"
  val KUDU = s"${kuduMaster}:${kuduPort}"

  var hdfsServer : String = "cloudera-vm.c.endless-upgrade-187216.internal"
  var hdfsPort : String = "8020"
  val HDFS = s"${hdfsServer}:${hdfsPort}"

  var kuduContext : KuduContext = null
  var spark : SparkSession = null

  var hdfs : FileSystem = null


  def init() : Unit = {
    init("local[*]", "Movie Recommender")
  }

  def init(master : String, appName : String) : Unit = {
    spark = SparkSession.builder().master(master).appName(appName).getOrCreate()
  }

  def initKudu(master : String, port : String) : Unit = {

    kuduMaster = master
    kuduPort = port
    kuduContext = new KuduContext(KUDU, spark.sparkContext)
  }

  def initKudu(master : String, port : String, sparkSession : SparkSession) : Unit = {

    if(spark == null)
      spark = sparkSession

    kuduContext = new KuduContext(KUDU, spark.sparkContext)
  }

  def initHDFS(server : String, port : String) : Unit = {

    hdfsServer = server
    hdfsPort = port

    val hadoopConfig = new Configuration()
    hadoopConfig.set("fs.defaultFS", HDFS)
    hdfs = FileSystem.get(hadoopConfig)
  }

  def insertKuduRows(rows : DataFrame, table : String) : Unit = {

    val tableName = s"impala::${table}"
    kuduContext.insertRows(rows, tableName)

  }

  def updateKuduRows(rows : DataFrame, table : String) : Unit = {

    val tableName = s"impala::${table}"
    kuduContext.updateRows(rows, tableName)

  }

  def deleteKuduRows(keys : DataFrame, table : String) : Unit = {

    val tableName = s"impala::${table}"
    kuduContext.deleteRows(keys, tableName)

  }

  def readKuduTable(kuduTestTable: String): DataFrame ={

    val df = spark.sqlContext.read.options(
      Map(
        "kudu.master" -> KUDU,
        "kudu.table" -> s"impala::${kuduTestTable}"
      )
    ).kudu

    df
  }

  def storeHDFSFile(filePath : String, fileName : String, data : Array[Byte]) = {

    val path = new Path(filePath)

    if(!hdfs.exists(path))
      hdfs.mkdirs(path)

    val fullPath = new Path(filePath+"/"+fileName)

    hdfs.create(fullPath).write(data)
  }

  def readHDFSFile(filePath : String) : Array[Byte] = {


    return null
  }


  def remoteSecureCopy(inputFile : String,
                       remoteUser : String, remoteHost : String, remotePath : String) : String = {

    s"scp -v ${inputFile} ${remoteHost}@${remoteHost}:/${remotePath}" !!
  }


  def zipModel(sourceDir : String, outName : String) : Unit = {
    ZipUtil.pack(new File(sourceDir), new File(outName))
  }

  def unzipModel(zipFile : String, outDir : String) : Unit = {
    ZipUtil.unpack(new File(zipFile), new File(outDir))
  }

  def closeSession() : Unit = {

    if(hdfs != null){
      hdfs.close()
      hdfs = null
    }

    if(spark != null) {
      spark.close()
      spark = null
    }

    kuduContext = null
  }







}
