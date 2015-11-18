package fileio

import configuration.ApplicationProperties
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

class AppData {

  val properties = new ApplicationProperties
  var s3FileIOClient : S3FileIOClient = null
  var adamdbFilePath = ""
  var appnameFilePath = ""
  var bermandbFilePath = ""
  var clusterFilePath = ""
  var cuigraphFilePath = ""
  var englishwordsdbFilePath = ""
  var inputbucket = ""
  var inputsfolder = ""
  var lrabrdbFilePath = ""
  var measurementsFilePath = ""
  var metamapFilePath = ""
  var outputbucket = ""
  var stopwordsFilePath = ""



  def initializeAppData(): Unit ={

    s3FileIOClient = new S3FileIOClient()
    s3FileIOClient.initializeS3Client()
    properties.initializeProperties(s3FileIOClient)

    adamdbFilePath = properties.adamdb
    appnameFilePath = properties.appname
    bermandbFilePath = properties.bermandb
    clusterFilePath = properties.cluster
    cuigraphFilePath = properties.cuigraph
    englishwordsdbFilePath = properties.englishwordsdb
    inputbucket = properties.inputbucket
    inputsfolder = properties.inputsfolder
    lrabrdbFilePath = properties.lrabrdb
    measurementsFilePath = properties.measurements
    metamapFilePath = properties.metamap
    outputbucket = properties.outputbucket
    stopwordsFilePath = properties.stopwords
  }


  def adamdb(): String = {
    s3FileIOClient.getObjectContentsAsString(inputbucket, adamdbFilePath)
  }

  def bermandb(): String = {
    s3FileIOClient.getObjectContentsAsString(inputbucket, bermandbFilePath)
  }

  def cuigraph(): String = {
    s3FileIOClient.getObjectContentsAsString(inputbucket, cuigraphFilePath)
  }

  def englishwordsdb(): String = {
    s3FileIOClient.getObjectContentsAsString(inputbucket, englishwordsdbFilePath)
  }

  def lrabrdb(): String = {
    s3FileIOClient.getObjectContentsAsString(inputbucket, lrabrdbFilePath)
  }

  def measurements(): String = {
    s3FileIOClient.getObjectContentsAsString(inputbucket, measurementsFilePath)
  }

  def metamap(): String = {
    s3FileIOClient.getObjectContentsAsString(inputbucket, metamapFilePath)
  }

  def stopwords(): String = {
    s3FileIOClient.getObjectContentsAsString(inputbucket, stopwordsFilePath)
  }

  def allInputNotes(): ListBuffer[String] = {
    val list = s3FileIOClient.listAllInputObjects(inputbucket, inputsfolder)
    list.filter(!_.endsWith("/"))
  }

  def inputNote(inputNote: String): String = {
    val result = s3FileIOClient.getObjectContentsAsString(inputbucket, inputNote)
    result
  }

  def loadObjectFromS3ToSparkRDD(sc: SparkContext) : RDD[String] = {

    // val uri = "s3n://"+ accessKey + ":" + secretKey +"@" + bucketName + "/" + objectKey

    val uri = "s3n://" + inputbucket + "/" + cuigraphFilePath

    val inputRDD = sc.textFile(uri)

    inputRDD

  }


  def loadMetamapFromS3ToSparkRDD(sc: SparkContext) : RDD[String] = {

    // val uri = "s3n://"+ accessKey + ":" + secretKey +"@" + bucketName + "/" + objectKey

    val uri = "s3n://" + inputbucket + "/" + metamapFilePath

    val inputRDD = sc.textFile(uri)

    inputRDD

  }

    def shutDownClient(): Unit = {
    s3FileIOClient.suhtDown()
  }

}
