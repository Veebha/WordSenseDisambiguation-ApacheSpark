package abbrdetection

import fileio.AppData
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import parsers.ParserAndMapBuilder

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

class AbbreviationDetection {

  println("Initialization started...  " + System.currentTimeMillis() )

  var appData : AppData = null
  val abbrMap = new mutable.HashMap[String, Byte]
  var englishWordsMap = new mutable.HashMap[String,Byte]
  var measurementsMap = new mutable.HashMap[String,Byte]
  var stopWordsMap = new mutable.HashMap[String,Byte]
  val dictionary = new mutable.HashMap[String, ListBuffer[String]]()
  var parsedMap = new mutable.HashMap[String, Byte]


 def loadAbbrDetectionMaps() : Unit = {

   parseAdamDB()
   parseLrAbrDB()
   parseTheBermenDB()
   buildBermanDictionary()

   val measurements = loadMeasurements()
   val englishWords = loadValidEnglishWords()
   val stopWords = loadStopWords()

   val keyList = parsedMap.keys.toList
   val masterList = keyList.sorted
   var filteredList = masterList.filter(measurements.contains(_) == false)
     .filter(englishWords.contains(_) == false)
     .filter(_.length > 1)

   measurements.foreach(measurementsMap.put(_,0))
   filteredList.foreach(abbrMap.put(_, 0))
   stopWords.foreach(stopWordsMap.put(_,0))

   parsedMap = null
   filteredList = null

   System.gc()

   println("Initialization completed... " + System.currentTimeMillis())
 }

  /****************************************************************************/

  def loadAbbrMap(): mutable.HashMap[String, Byte] ={
    abbrMap
  }

  def loadEnglishWordsMap(): mutable.HashMap[String, Byte] ={
    englishWordsMap
  }

  def loadMeasurementsMap(): mutable.HashMap[String, Byte] ={
    measurementsMap
  }

  def loadStopWordsMap(): mutable.HashMap[String, Byte] ={
    stopWordsMap
  }

  def loadBermanDictionary(): mutable.HashMap[String, ListBuffer[String]] ={
    dictionary
  }

  /****************************************************************************/

  def buildBermanDictionary(): Unit ={

    val lines = appData.bermandb().split("\n")

    lines.foreach({ line =>

      val tokens = line.split(" = ")
      if(tokens.nonEmpty){
        val key: String = tokens(0).trim
        if(tokens.size >1) {
          val meaning: String = tokens(1).trim
          if (dictionary.get(key).isDefined) {
            val list = dictionary.get(key).get
            list.append(meaning)
          } else {
            val newList = new ListBuffer[String]
            newList.append(meaning)
            dictionary.put(key, newList)
          }
        }
      }
    })
  }

  /****************************************************************************/

  def parseAdamDB(): Unit = {
    val lines = appData.adamdb().split("\n")
    lines.foreach({ line =>
      if (line(0) != '#') {
        val tokens = line.split("\t")
        parsedMap.put(tokens(0).toLowerCase, 0)
      }
    })
  }

  def parseLrAbrDB(): Unit = {
    val lines = appData.lrabrdb().split("\n")
    lines.foreach({ line =>
      val tokens = line.split('|')
      parsedMap.put(tokens(1).toLowerCase, 0)
    })
  }

  def parseTheBermenDB(): Unit = {
    val lines = appData.bermandb().split("\n")
    lines.foreach({ line =>
      val tokens: Array[String] = line.split(" = ")
      parsedMap.put(tokens(0).toLowerCase, 0)
    })
  }

  def loadValidEnglishWords(): ListBuffer[String] = {
    val validEnglishWords = new ListBuffer[String]
    val words = appData.englishwordsdb().split("\n")
    words.foreach({ word => validEnglishWords.append(word.toLowerCase)
      englishWordsMap.put(word,0)
    })
    validEnglishWords
  }

  def loadMeasurements(): ListBuffer[String] = {
    val measurements = new ListBuffer[String]
    val items = appData.measurements().split("\n")
    items.foreach({ item => measurements.append(item.toLowerCase)
    })
    measurements
  }

  def loadStopWords() : ListBuffer[String] ={
    val stopWords = new ListBuffer[String]
    val lines = appData.stopwords().split("\n")
    lines.foreach({ word =>
      stopWords.append(word.toLowerCase)
    })
    stopWords
  }

  /****************************************************************************/

  def readContents(source: String): List[String] = {
    Source.fromFile(source).getLines().toList
  }

  /****************************************************************************/

  def initializeAbbrDetectionEngine(sc: SparkContext, appDataObject:AppData) : Boolean ={

    appData = appDataObject

    loadAbbrDetectionMaps()

    val abbreviations = loadAbbrMap()
    val englishWords = loadEnglishWordsMap()
    val stopWords = loadStopWordsMap()
    val measurements = loadMeasurementsMap()
    val dictionary = loadBermanDictionary()

    val broadCastedObjects: Broadcast[(mutable.HashMap[String, Byte], mutable.HashMap[String, Byte], mutable.HashMap[String, Byte], mutable.HashMap[String, Byte], mutable.HashMap[String, ListBuffer[String]])] = sc.broadcast(abbreviations, englishWords, stopWords, measurements, dictionary)

    ParserAndMapBuilder.broadCastedObjects = broadCastedObjects

    true
  }

}