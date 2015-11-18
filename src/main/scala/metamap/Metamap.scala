package metamap

import fileio.AppData
import org.apache.spark.SparkContext
import parsers.MetaMapParser

object Metamap {

  def buildMetamap(sc: SparkContext, appData: AppData)  ={
    val metaMapContentsRDD = appData.loadMetamapFromS3ToSparkRDD(sc)
    val parsedMetamap = metaMapContentsRDD.map({line => MetaMapParser.parse(line) })
    parsedMetamap.sortByKey(ascending = true)
  }
}
