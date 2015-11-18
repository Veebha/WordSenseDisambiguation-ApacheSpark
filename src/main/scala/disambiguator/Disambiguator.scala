package disambiguator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Disambiguator {

  type ListAbbrCUIListContextWordStringsTuples = ListBuffer[(String, ListBuffer[Long], String)]
  type ListAbbrCUIListContextWordCUIListTuples = ListBuffer[(String, ListBuffer[Long], ListBuffer[Long])]
  type ListAbbrCUIListContexWordCUIListAbbrCUINeighborListTuples = ListBuffer[(String, ListBuffer[Long], ListBuffer[ListBuffer[Long]], ListBuffer[ListBuffer[Long]])]

  def buildAbbrCuiListContextStringTuples(abbrContextMap: mutable.HashMap[String, String], masterCuiListMapForNote: mutable.HashMap[String, ListBuffer[Long]]): ListAbbrCUIListContextWordStringsTuples = {

    val listOfTuplesWithAbbrCuiListContextWords = new ListAbbrCUIListContextWordStringsTuples
    abbrContextMap.foreach({ entry =>
      val key = entry._1
      var listOfCUIsForAbbr = new ListBuffer[Long]
      if (masterCuiListMapForNote.get(key).isDefined) {
        listOfCUIsForAbbr = masterCuiListMapForNote.get(key).get
        val contextWords = entry._2
        listOfTuplesWithAbbrCuiListContextWords.append((key, listOfCUIsForAbbr, contextWords))
      }
    })
    listOfTuplesWithAbbrCuiListContextWords
  }

  def buildListAbbrCUIListContextWordCUIListTuples(listOfTuplesWithAbbrCuiListContextWords: ListAbbrCUIListContextWordStringsTuples, masterCuiListMapForNote: mutable.HashMap[String, ListBuffer[Long]]): ListAbbrCUIListContextWordCUIListTuples = {

    val listOfTuplesWithAbbrCuiListContextWordsCuiList = new ListAbbrCUIListContextWordCUIListTuples

    listOfTuplesWithAbbrCuiListContextWords.foreach({ entry =>

      val key = entry._1
      val listOfCUIsForAbbr = entry._2
      val contextWords = entry._3
      val listofContextWordCuis = new ListBuffer[Long]

      val contextWordTokens = contextWords.split(",")
      contextWordTokens.foreach({ contextWord =>

        var listOfCUIsForCurContextWord = new ListBuffer[Long]
        if (masterCuiListMapForNote.get(contextWord).isDefined) {
          listOfCUIsForCurContextWord = masterCuiListMapForNote.get(contextWord).get
          if(listOfCUIsForCurContextWord != null) {
            listOfCUIsForCurContextWord.foreach({ entry => listofContextWordCuis.append(entry) })
          }
        }
      })
      listOfTuplesWithAbbrCuiListContextWordsCuiList.append((key, listOfCUIsForAbbr, listofContextWordCuis))
    })
    listOfTuplesWithAbbrCuiListContextWordsCuiList
  }

}