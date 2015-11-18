package graph

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object GraphBrowser {

  def getNeighborsForAbbrCuis(listOfCuis: List[ListBuffer[(String, ListBuffer[Long], ListBuffer[ListBuffer[Long]])]]) : mutable.HashMap[Long,ListBuffer[Long]]  = {

    val mapOfCuisToGetNeighborsFor = new mutable.HashMap[Long,ListBuffer[Long]]
    listOfCuis.foreach({listOfTuples =>
      listOfTuples.foreach({tuple =>
       // println(tuple)
        if(tuple._2 != null) {
          val listOfCuisForAbbr = tuple._2
          listOfCuisForAbbr.foreach({ cuiEntry =>
            mapOfCuisToGetNeighborsFor.put(cuiEntry, null)
          }) } }) })

    mapOfCuisToGetNeighborsFor.foreach({ mapEntry =>
      val listOfNeighbors = GraphObject.getNeighborsList(mapEntry._1)
      if (listOfNeighbors.nonEmpty) {
        mapOfCuisToGetNeighborsFor.put(mapEntry._1, listOfNeighbors)
      }
    })

    mapOfCuisToGetNeighborsFor
  }

}
