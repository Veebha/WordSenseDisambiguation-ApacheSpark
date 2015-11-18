package metamap

import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object MetamapBrowser {

  var metamap: Broadcast[mutable.HashMap[String, ListBuffer[Long]]] = null

  def setMetamapBroadcasObject(broadcastObject: Broadcast[mutable.HashMap[String, ListBuffer[Long]]]): Unit = {
    metamap = broadcastObject
  }

  def loadCUIsForAllTokens(tokens: ListBuffer[String]): mutable.HashMap[String, ListBuffer[Long]] = {

    val cuiMapToLoad = new mutable.HashMap[String, ListBuffer[Long]]
    tokens.foreach({ word =>
      cuiMapToLoad.put(word, getWordEntryFromMetamap(word))
    })
    cuiMapToLoad
  }

  def getWordEntryFromMetamap(word: String): ListBuffer[Long] = {

    val result: Option[ListBuffer[Long]] = metamap.value.get(word)

    if (result.isEmpty) {
      return null
    }
    result.get
  }

}
