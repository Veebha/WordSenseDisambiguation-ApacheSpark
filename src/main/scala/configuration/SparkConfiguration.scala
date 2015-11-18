package configuration

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}

object SparkConfiguration {

  var conf: SparkConf = null
  var context: SparkContext = null

  def initializeSpark(appName: String, master: String) {

    conf = new SparkConf()
      .setAppName(appName)
      .setMaster(master)
      .set("spark.driver.memory", "4G")
      .set("spark.executor.memory", "4G")
    context = new SparkContext(conf)
    println("\n\n Context Generated !\n\n")
  }

  def configureSparkContextForS3Access(): Unit = {


    val properties = new Properties()
    val propertiesFilePath = getClass.getResource("/client.properties").getPath
    val input = new FileInputStream(propertiesFilePath)
    properties.load(input)
    val accessKey = properties.getProperty("accessKey")
    val secretKey = properties.getProperty("secretKey")

    val hadoopConf = context.hadoopConfiguration
    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoopConf.set("fs.s3n.awsAccessKeyId", accessKey)
    hadoopConf.set("fs.s3n.awsSecretAccessKey", secretKey)
  }

  def getConfiguredSpark: SparkContext = {
     context
  }
}