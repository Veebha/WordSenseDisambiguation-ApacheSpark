package configuration

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.StandardCharsets
import java.util.Properties

import fileio.S3FileIOClient

class ApplicationProperties {

  private var properties = new Properties()
  private var propertiesContents = ""
  private var input: InputStream = null

  var appname = ""
  var cluster = ""
  var englishwordsdb = ""
  var adamdb = ""
  var bermandb =""
  var lrabrdb = ""
  var measurements = ""
  var metamap = ""
  var stopwords = ""
  var cuigraph = ""
  var inputbucket = ""
  var outputbucket= ""
  var inputsfolder= ""

  def initializeProperties(s3FileIOClient: S3FileIOClient) : Boolean = {

    println("Properties are being initialized !!!!")

    properties = new Properties()
    propertiesContents = s3FileIOClient.getObjectContentsAsString("mastersprojectwsdinputs", "Configuration/config.properties")
    try {
      input = new ByteArrayInputStream(propertiesContents.getBytes(StandardCharsets.UTF_8))
      properties.load(input)
      loadProperties()
      true
    } catch {
      case io: Exception =>
        io.printStackTrace()
        false
    }
  }

  def loadProperties() : Unit = {
    appname = properties.getProperty("appname")
    cluster = properties.getProperty("cluster")
    englishwordsdb = properties.getProperty("englishwordsdb")
    adamdb =  properties.getProperty("adamdb")
    bermandb = properties.getProperty("bermandb")
    lrabrdb = properties.getProperty("lrabrdb")
    measurements = properties.getProperty("measurements")
    metamap = properties.getProperty("metamap")
    stopwords  = properties.getProperty("stopwords")
    cuigraph = properties.getProperty("cuigraph")
    inputbucket = properties.getProperty("input.bucket")
    outputbucket = properties.getProperty("output.bucket")
    inputsfolder = properties.getProperty("inputs.folder")
  }
}
