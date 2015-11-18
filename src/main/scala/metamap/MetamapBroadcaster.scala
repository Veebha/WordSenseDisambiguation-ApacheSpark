package metamap

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object MetamapBroadcaster {

  def broadcastMetamap(sc: SparkContext, loadedMap: RDD[(String, ListBuffer[Long])], flag: Boolean): Unit = {

    val wordsToCuiMap = new mutable.HashMap[String, ListBuffer[Long]]()
    loadedMap.collect().foreach({ entry =>
      wordsToCuiMap.put(entry._1, entry._2)
    })

    val metamap = sc.broadcast(wordsToCuiMap)
    MetamapBrowser.setMetamapBroadcasObject(metamap)
    if (flag) {
      metamap.unpersist()
      MetamapBrowser.setMetamapBroadcasObject(null)
    }
  }
}
