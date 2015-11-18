package similarity.calculation

import scala.collection.mutable.ListBuffer
import scala.collection.mutable

object Similarity {


  def calculateSimilarity(abbr: String, abbrCuiList: ListBuffer[Long], abbrCuiNeighborNodesList: ListBuffer[ListBuffer[Long]], contextWordCuilList: ListBuffer[Long]) = {

    val simMapForEachCUi = new mutable.HashMap[Long, Double]

    abbrCuiList.foreach({ abbrCui =>
      val index = abbrCuiList.indexOf(abbrCui)
      val listOfNeighborNodes = abbrCuiNeighborNodesList(index)
      if (listOfNeighborNodes.nonEmpty) {
        val similarity = cosineSimilarity(listOfNeighborNodes, contextWordCuilList)
        simMapForEachCUi.put(abbrCui, similarity)
      }
    })
    var cuiOfMaxSimiliarity: Long = 0
    var maxSim: Double = 0.0
    simMapForEachCUi.foreach({ entry =>
      if (entry._2 > maxSim) {
        cuiOfMaxSimiliarity = entry._1
        maxSim = entry._2
      }
    })

    (abbr, cuiOfMaxSimiliarity, maxSim)
  }

  def jaccardSimilarity(listOfNeighborNodes: ListBuffer[Long], masterListOfContextWordCuis: ListBuffer[Long]): Double = {

    val intersect = listOfNeighborNodes.intersect(masterListOfContextWordCuis)

    val unionPrepOne = listOfNeighborNodes.diff(masterListOfContextWordCuis)
    val unionPrepTwo = masterListOfContextWordCuis.diff(listOfNeighborNodes)

    val union = intersect.union(unionPrepOne).union(unionPrepTwo)

    if (union.isEmpty) 1.0
    else intersect.size.toDouble / union.size.toDouble

  }

  def cosineSimilarity(listOfNeighborNodes: ListBuffer[Long], masterListOfContextWordCuis: ListBuffer[Long]): Double = {

    val intersect = listOfNeighborNodes.intersect(masterListOfContextWordCuis)

    val unionPrepOne = listOfNeighborNodes.diff(masterListOfContextWordCuis)
    val unionPrepTwo = masterListOfContextWordCuis.diff(listOfNeighborNodes)

    val union = intersect.union(unionPrepOne).union(unionPrepTwo)

    if (listOfNeighborNodes.isEmpty || masterListOfContextWordCuis.isEmpty ) 0.0
    else intersect.size.toDouble / ( math.sqrt(listOfNeighborNodes.size) * math.sqrt(masterListOfContextWordCuis.size))

  }
}
