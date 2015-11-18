package parsers

import fileio.AppData
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

object ParserAndMapBuilder {

  type broadcast = Broadcast[(mutable.HashMap[String, Byte], mutable.HashMap[String, Byte], mutable.HashMap[String, Byte], mutable.HashMap[String, Byte], mutable.HashMap[String, ListBuffer[String]])]

  var broadCastedObjects: broadcast = null

  def parse(source: String): ListBuffer[String] = {

    val appData = new AppData
    appData.initializeAppData()

    val parsedTokens = new ListBuffer[String]
    val contents = appData.inputNote(source).split("\n")

    contents.foreach({
      line =>
        val words = line.split(" ")
        words.foreach({ word =>

          val subWords = word.split(",")

          subWords.foreach({ text =>
            val textOne = text.stripPrefix(".~`!@#$%^&*()-_=+[]{}|:;\",.<>?/")
            val textToken = textOne.stripSuffix(".~`!@#$%^&*()-_=+[]{}|:;\",.<>?/")

            if (!validateStopWords(textToken) && (!validateEnglishWord(textToken))) {
              parsedTokens.append(textToken.toLowerCase)
            }

          })

        })
    })
    appData.shutDownClient()
    parsedTokens
  }

  def detectAbbreviations(parsedTokens: ListBuffer[String]): ListBuffer[String] = {

    val abbreviations = new ListBuffer[String]

    parsedTokens.foreach({ token =>
      if (validateAbbreviation(token) && (!validateEnglishWord(token))) {
        if (!abbreviations.contains()) {
          abbreviations.append(token)
        }
      }
    })
    abbreviations
  }

  def buildCandidateContextTokensList(parsedTokens: ListBuffer[String], candidateAbbrs: List[String]) = {

    val candidateContextTokens = new ListBuffer[String]

    candidateAbbrs.foreach({ token =>
      val index = parsedTokens.indexOf(token)
      if ((parsedTokens.length - 1) - index == 0) {
        var counter = index - 1
        var inc = 0
        while (counter >= 0 && inc < 10) {

          if (!candidateContextTokens.contains(parsedTokens(counter))) {
            candidateContextTokens.append(parsedTokens(counter))
          }
          counter -= 1
          inc += 1
        }
      }

      else if (index == 0) {
        var counter = index + 1
        while (counter <= 10 && counter < parsedTokens.length) {
          if (!candidateContextTokens.contains(parsedTokens(counter))) {
            candidateContextTokens.append(parsedTokens(counter))
          }
          counter += 1
        }

      }

      else {
        var counter = index + 1
        var inc = 0

        while (inc < 5 && counter + inc < parsedTokens.length) {
          if (!candidateContextTokens.contains(parsedTokens(counter + inc))) {
            candidateContextTokens.append(parsedTokens(counter + inc))
          }
          inc += 1
        }

        counter = index - 1
        var dec = 0
        var flag = 0
        while (counter - dec >= 0 && flag < 5) {
          if (!candidateContextTokens.contains(parsedTokens(counter - dec))) {
            candidateContextTokens.append(parsedTokens(counter - dec))
          }
          dec += 1
          flag += 1
        }

      }
    })
    candidateContextTokens
  }

  def buildMapOfAbbrStringContextStringList(parsedTokens: ListBuffer[String], candidateAbbrs: List[String]) = {

    val mapAbbrContextWordStringList = new mutable.HashMap[String, ListBuffer[String]]

    candidateAbbrs.foreach({ token =>
      val index = parsedTokens.indexOf(token)

      if ((parsedTokens.length - 1) - index == 0) {
        val candidateContextTokens = new ListBuffer[String]
        var counter = index - 1
        var inc = 0
        while (counter >= 0 && inc < 10) {
          if (!candidateContextTokens.contains(parsedTokens(counter))) {
            candidateContextTokens.append(parsedTokens(counter))
          }
          counter -= 1
          inc += 1
        }
        mapAbbrContextWordStringList.put(token, candidateContextTokens)
      }

      else if (index == 0) {
        val candidateContextTokens = new ListBuffer[String]
        var counter = index + 1
        while (counter <= 10 && counter < parsedTokens.length) {
          if (!candidateContextTokens.contains(parsedTokens(counter))) {
            candidateContextTokens.append(parsedTokens(counter))
          }
          counter += 1
        }
        mapAbbrContextWordStringList.put(token, candidateContextTokens)
      }

      else {
        val candidateContextTokens = new ListBuffer[String]
        var counter = index + 1
        var inc = 0

        while (inc < 5 && counter + inc < parsedTokens.length) {
          if (!candidateContextTokens.contains(parsedTokens(counter + inc))) {
            candidateContextTokens.append(parsedTokens(counter + inc))
          }
          inc += 1
        }

        counter = index - 1
        var dec = 0
        var flag = 0
        while (counter - dec >= 0 && flag < 5) {
          if (!candidateContextTokens.contains(parsedTokens(counter - dec))) {
            candidateContextTokens.append(parsedTokens(counter - dec))
          }
          dec += 1
          flag += 1
        }
        mapAbbrContextWordStringList.put(token, candidateContextTokens)
      }

    })
    mapAbbrContextWordStringList
  }

  def loadContextWordCuiList(mapAbbrContextWordsList: mutable.HashMap[String, ListBuffer[String]], mapContextWordCuis: mutable.HashMap[String, ListBuffer[Long]]) = {

    val mapAbbrContextCuiList = new mutable.HashMap[String, ListBuffer[Long]]

    mapAbbrContextWordsList.foreach({ entry =>

      val key = entry._1
      val contextWords = entry._2
      val contextCuis = new ListBuffer[Long]

      contextWords.foreach({ word =>

        val list = mapContextWordCuis.get(word).get
        if (list != null) {
          list.foreach({ cui =>
            if (!contextCuis.contains(cui)) {
              contextCuis.append(cui)
            }
          })
        }
      })

      mapAbbrContextCuiList.put(key, contextCuis)
    })
    mapAbbrContextCuiList
  }


  def buildTupleListOfAbbrCuiContextCui(clinicalNote: String, mapAbbreVationsWithMultipleCuis: mutable.HashMap[String, ListBuffer[Long]], mapAbbrContextWordsCuiList: mutable.HashMap[String, ListBuffer[Long]]) = {
    val aggregatedTupleList = new ListBuffer[(String, ListBuffer[Long], ListBuffer[Long])]
    mapAbbreVationsWithMultipleCuis.foreach({ entry =>
      aggregatedTupleList.append((clinicalNote + "-" + entry._1, entry._2, mapAbbrContextWordsCuiList.get(entry._1).get))
    })
    aggregatedTupleList
  }

  def validateAbbreviation(abbr: String): Boolean = {
    broadCastedObjects.value._1.get(abbr.toLowerCase).isDefined
  }

  def validateEnglishWord(abbr: String): Boolean = {
    broadCastedObjects.value._2.get(abbr.toLowerCase).isDefined
  }

  def validateStopWords(abbr: String): Boolean = {
    broadCastedObjects.value._3.get(abbr.toLowerCase).isDefined
  }

  def validateMeasurement(abbr: String): Boolean = {
    broadCastedObjects.value._4.get(abbr.toLowerCase).isDefined
  }

  def readContents(source: String): List[String] = {
    Source.fromFile(source).getLines().toList
  }

}