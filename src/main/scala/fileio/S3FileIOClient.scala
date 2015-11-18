package fileio

import java.io.{FileInputStream, InputStream}
import java.util.Properties

import com.amazonaws.{ClientConfiguration, Protocol}
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model._
import org.apache.commons.io.IOUtils

import scala.collection.mutable.ListBuffer

class S3FileIOClient  {


  var s3Client: AmazonS3Client = null

  var accessKey = ""
  var secretKey = ""


  def initializeS3Client(): Boolean = {
    loadClientProperties()
    val credentials = new BasicAWSCredentials(accessKey, secretKey)
    val clientConfig = new ClientConfiguration()
    clientConfig.setProtocol(Protocol.HTTP)
    s3Client = new AmazonS3Client(credentials, clientConfig)
    true
  }

  def listAllInputObjects(bucket: String, folder: String) : ListBuffer[String] ={
    val objectKeyList = new ListBuffer[String]
    try {
      var objectList = listObjects(bucket, folder)
      do {
        val iteratorForObjectSummaries = objectList.getObjectSummaries.iterator()
        while (iteratorForObjectSummaries.hasNext) {
          val entry = iteratorForObjectSummaries.next()
          objectKeyList.append(entry.getKey)
        }
        objectList = listNextBatchOfObjects(objectList)
      } while (objectList.isTruncated)
    } catch {
      case exception: Exception => exception.printStackTrace()
    }
    objectKeyList
  }

  def listObjects(filtersBucket: String, filtersFolder: String) : ObjectListing = {
    s3Client.listObjects(filtersBucket, filtersFolder)
  }

  def listNextBatchOfObjects(objectList: ObjectListing) : ObjectListing = {
    s3Client.listNextBatchOfObjects(objectList)
  }

  def getObjectContentsAsString(bucket: String, objectKey: String) : String = {
      val s3Object = s3Client.getObject(new GetObjectRequest(bucket, objectKey))
      val objectData = s3Object.getObjectContent
      IOUtils.toString(objectData, "UTF-8")
    }

  def loadClientProperties() : Unit = {
    val properties = new Properties()
    var input : InputStream = null

    try{
      val propertiesFilePath = getClass.getResource("/client.properties").getPath
      input = new FileInputStream(propertiesFilePath)
      properties.load(input)
      accessKey = properties.getProperty("accessKey")
      secretKey = properties.getProperty("secretKey")
    }
  }

  def suhtDown(): Unit = {
    s3Client.shutdown()
  }

}
