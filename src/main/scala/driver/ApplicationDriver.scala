package driver

import abbrdetection.AbbreviationDetection
import configuration. SparkConfiguration
import fileio.AppData
import graph.GraphObject
import metamap.{MetamapBrowser, MetamapBroadcaster, Metamap}
import metrics.Metrics
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import parsers.ParserAndMapBuilder
import similarity.calculation.Similarity
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import org.slf4j.LoggerFactory

object ApplicationDriver extends App {

  var callToPreProcess = 0
  val logger = LoggerFactory.getLogger(this.getClass)

  type ListAbbr_CuiList_ContextWordCUIListTuples = ListBuffer[(String, ListBuffer[Long], ListBuffer[Long])]
  println("\n\n\n Step 1 : Start ")
  Metrics.ApplicationStartTime = System.currentTimeMillis()
  // Metrics

  val appData = new AppData
  appData.initializeAppData()
  val properties = appData.properties

  SparkConfiguration.initializeSpark(properties.appname, properties.cluster)
  SparkConfiguration.configureSparkContextForS3Access()
  val sc = SparkConfiguration.getConfiguredSpark


  val abbreviationDetection = new AbbreviationDetection
  abbreviationDetection.initializeAbbrDetectionEngine(sc, appData)

  val loadedMetamap = Metamap.buildMetamap(sc, appData)
  var flag = false
  MetamapBroadcaster.broadcastMetamap(sc, loadedMetamap, flag)

  println("\n\n\n Step 1 : Complete ")


  /** ********************************************************************************************************************/

  println("\n\n\n Step 2 : Start ")

  val inputNotes = appData.allInputNotes()

  val list: RDD[String] = sc.parallelize(inputNotes)

  val mapOfDisambiguatedEntriesInFirstStep = new mutable.HashMap[String, Long]()

  Metrics.SparkEngineStartTime = System.currentTimeMillis()
  // Metrics

  val preProcessDump = new ListBuffer[(String, ListBuffer[Long], ListBuffer[Long])]

  val listOfTuplesToDistribute = new ListBuffer[(String, ListBuffer[Long], ListBuffer[Long])]

  println("\n\n\n Count of list is : " + list.count() + "\n\n\n")

  val abbrCUIListContextWordCUIList = list.map(preprocess) // worker processing


  val collectedList = abbrCUIListContextWordCUIList.collect()


  val collectedAbbrCUIListContextWordCUIList = collectedList.foreach({entry=>
    entry.foreach({tuple =>
      listOfTuplesToDistribute.append(tuple)
    })
  })

  Metrics.SparkEngineEndTIme = System.currentTimeMillis() // Metrics

  println("\n\n\n Step 2 : Complete ")

  /**********************************************************************************************************************/

  Metrics.GraphLoadingStartTime = System.currentTimeMillis()         // Metrics

  GraphObject.loadGraph(sc, appData)

  Metrics.GraphLoadingEndTime = System.currentTimeMillis()          // Metrics

  println("\n\n\n Step 3 : Complete ")

  /**********************************************************************************************************************/

  Metrics.SprakGraphScanStart = System.currentTimeMillis()

  val mapOfCuiAndNeighborNodes = GraphObject.getNeighborsForAbbrAndContextCuis(collectedList.toList)
  val graphMap: Broadcast[mutable.HashMap[Long, ListBuffer[Long]]] = sc.broadcast(mapOfCuiAndNeighborNodes)

  Metrics.SprakGraphScanStop = System.currentTimeMillis()

  println("\n\n\n##############  Graph Load ##############")

  // println(mapOfCuiAndNeighborNodes.size)

  println("\n\n\n##############  Graph Load ##############")

  Metrics.GraphLoadingEndTime = System.currentTimeMillis()

  println("\n\n\n Step 4 : Complete ")

  /**********************************************************************************************************************/


  val listOfTuplesToDistributeRDD = sc.parallelize(listOfTuplesToDistribute)

  println("\n\n\n Step 5 : Start ")

  val abbrCUIWithNeighborsListContextWordCUIList: RDD[(String, ListBuffer[Long], ListBuffer[ListBuffer[Long]], ListBuffer[Long])] = listOfTuplesToDistributeRDD.map({listEntry => loadAbbrNeighbors(listEntry)})
  abbrCUIWithNeighborsListContextWordCUIList.cache()
  println("\n\n\n Step 5 : Complete ")


  println("\n\n\n Step 23 : Start ")

  val abbrCUIWithNeighborsListContextWordNeighborsCUIList = abbrCUIWithNeighborsListContextWordCUIList.map({listEntry => loadContextWordNeighbors(listEntry)})

  abbrCUIWithNeighborsListContextWordNeighborsCUIList.cache()
  println("\n\n\n Step 23 : Complete ")

  val mapOfDisambiguatedEntriesInSecondStep = new mutable.HashMap[String,Long]

  abbrCUIWithNeighborsListContextWordNeighborsCUIList.foreach({listEntry => computeSimiliarty(listEntry)})


  // abbrCUIWithNeighborsListContextWordCUIList.foreach({listEntry => computeSimiliarty(listEntry)})

  println("\n\n\n##############  Result 1 ##############")
  preProcessDump.foreach(println)
  println("\n\n\n##############  Result 1 ##############")

  println("\n\n\n Size  of preProcessDump is : " + preProcessDump.length + "\n\n\n" )


  println("\n\n\n##############  Result 2 ##############")

  mapOfDisambiguatedEntriesInSecondStep.foreach(println)

  val listOfNotDisambiguatedCuis = new ListBuffer[String]

  val listOfCuisDisambiguatedCuisFromTheDocument = new ListBuffer[Long]

  preProcessDump.foreach({entry =>
      if(entry._2 != null && entry._3 == null){
        listOfCuisDisambiguatedCuisFromTheDocument.append(entry._2.head)
      }
  })

  mapOfDisambiguatedEntriesInSecondStep.foreach({entry =>
    if(entry._2 != 0){
      listOfCuisDisambiguatedCuisFromTheDocument.append(entry._2)
    }
    else if(entry._2 == 0){
      listOfNotDisambiguatedCuis.append(entry._1.split("-")(1))
    }
  })


  /**********************************************************************************************************************/

                                                /* Step Two */

  /**********************************************************************************************************************/


  val mapOfDisambiguatedEntriesInLastStep = new mutable.HashMap[String,Long]


  val listOfneighborsForDisambiguatedCuis = GraphObject.getNeighborsForAllCuisInAList(listOfCuisDisambiguatedCuisFromTheDocument)
  val listOfDisambiguatedNeighborCuis = sc.broadcast(listOfneighborsForDisambiguatedCuis)

  /**********************************************************************************************************************/

  val listOfNotDisambigautedCuisRDD = sc.parallelize(listOfNotDisambiguatedCuis)

  val abbrCuisWithNeighbors = listOfNotDisambigautedCuisRDD.map(preProcessForSecondLevelDisambiguation)

  abbrCuisWithNeighbors.foreach(computeSimiliartyForUnDisambiguated)


  mapOfDisambiguatedEntriesInLastStep.foreach(println)


  appData.shutDownClient()

  Metrics.ApplicationEndTime = System.currentTimeMillis()           // Metrics



  Metrics.showMetrics()


  /** ********************************************************************************************************************/


  def preprocess(clinicalNote: String): ListBuffer[(String, ListBuffer[Long], ListBuffer[Long])] = {

    callToPreProcess += 1
    val noteNameArray = clinicalNote.split("/")
    val noteName = noteNameArray(noteNameArray.length - 1)

    val parsedTokens = ParserAndMapBuilder.parse(clinicalNote)

    val abbrevations = ParserAndMapBuilder.detectAbbreviations(parsedTokens)

    val mapAbbrevationsCuis = MetamapBrowser.loadCUIsForAllTokens(abbrevations)

    val mapAbbrevationWithCuis = mapAbbrevationsCuis.filter({ entry => entry._2 != null })

    val mapAbbrevationWithNoCuis = mapAbbrevationsCuis.filter({ entry => entry._2 == null })
    val mapAbbrevationsWithSingleCui = mapAbbrevationWithCuis.filter({ entry => entry._2.length == 1 })

    val mapAbbreVationsWithMultipleCuis = mapAbbrevationWithCuis.filter({ entry => entry._2.length > 1 })

    val lisOfCandidateContextTokens = ParserAndMapBuilder.buildCandidateContextTokensList(parsedTokens, mapAbbreVationsWithMultipleCuis.keySet.toList)

    val mapContextWordCuis = MetamapBrowser.loadCUIsForAllTokens(lisOfCandidateContextTokens)

    val mapAbbrContextWordsList = ParserAndMapBuilder.buildMapOfAbbrStringContextStringList(parsedTokens, mapAbbreVationsWithMultipleCuis.keySet.toList)

    val mapAbbrContextWordsCuiList = ParserAndMapBuilder.loadContextWordCuiList(mapAbbrContextWordsList, mapContextWordCuis)

    val listTupleAbbrCuiListContextWordCuiList = ParserAndMapBuilder.buildTupleListOfAbbrCuiContextCui(noteName, mapAbbreVationsWithMultipleCuis, mapAbbrContextWordsCuiList)

    mapAbbrevationsWithSingleCui.foreach({ entry =>
      preProcessDump.append((noteName + "-" + entry._1, entry._2, null))
    })

    mapAbbrevationWithNoCuis.foreach({ entry =>
      preProcessDump.append((noteName + "-" + entry._1, null, null))
    })

    listTupleAbbrCuiListContextWordCuiList

  }

  def computeSimiliarty(tupleEntry: (String, ListBuffer[Long], ListBuffer[ListBuffer[Long]], ListBuffer[Long])) ={

    val abbr = tupleEntry._1
    val listOfCuis = tupleEntry._2
    val listoFneighborsForCuis = tupleEntry._3
    val listOfContextWordCuis = tupleEntry._4

    val result = Similarity.calculateSimilarity(abbr, listOfCuis, listoFneighborsForCuis, listOfContextWordCuis)

    mapOfDisambiguatedEntriesInSecondStep.put(result._1, result._2)
  }

  def loadAbbrNeighbors(entry: (String, ListBuffer[Long], ListBuffer[Long])) = {

    val key = entry._1
    val listOfCUIsForAbbr = entry._2
    val listOfCUIsForCurContextWord = entry._3
    val listOfNeighboringNodesForEachCuiOfAbbr = new ListBuffer[ListBuffer[Long]]

    if(listOfCUIsForAbbr != null) {
      listOfCUIsForAbbr.foreach({ abbrCui =>
        val neighbors = getNeighborsList(abbrCui)
        neighbors.foreach({node =>
          val neighborsOfNeighbor = getNeighborsList(node)
          if(neighborsOfNeighbor.nonEmpty){
            neighborsOfNeighbor.foreach({entry =>
              if(!neighbors.contains(entry)){neighbors.append(entry)}
            })}
        })
        listOfNeighboringNodesForEachCuiOfAbbr.append(neighbors)
      })
    }
    (key,listOfCUIsForAbbr,listOfNeighboringNodesForEachCuiOfAbbr,listOfCUIsForCurContextWord)
  }

  def loadContextWordNeighbors(entry: (String, ListBuffer[Long], ListBuffer[ListBuffer[Long]],ListBuffer[Long])) = {

    val key = entry._1
    val listOfCUIsForAbbr = entry._2
    val listOfNeighborCUIsForAbbr = entry._3
    val listOfCUIsForCurContextWord = entry._4
    val listOfNighborNodesForEachContextWordCui = new ListBuffer[Long]

    listOfCUIsForCurContextWord.foreach({cui =>
      val neighbors = getNeighborsList(cui)
      if(neighbors.nonEmpty ) {
        neighbors.foreach({ entry => if (!listOfNighborNodesForEachContextWordCui.contains(entry)) {
          listOfNighborNodesForEachContextWordCui.append(entry)
        }
        })
      }
    })
    (key,listOfCUIsForAbbr,listOfNeighborCUIsForAbbr,listOfNighborNodesForEachContextWordCui)
  }

  def getNeighborsList(cui: Long): ListBuffer[Long] = {
    if(graphMap.value.get(cui).isDefined)
    {
      if(graphMap.value.get(cui).get == null) {return new ListBuffer[Long]}
      return graphMap.value.get(cui).get
    }
    new ListBuffer[Long]
  }

 def preProcessForSecondLevelDisambiguation(token: String) = {

   val listOfCUIsForAbbr = MetamapBrowser.getWordEntryFromMetamap(token)
   val listOfNeighboringNodesForEachCuiOfAbbr = new ListBuffer[ListBuffer[Long]]

   if(listOfCUIsForAbbr != null) {
     listOfCUIsForAbbr.foreach({ abbrCui =>
       val neighbors = getNeighborsList(abbrCui)
       neighbors.foreach({node =>
         val neighborsOfNeighbor = getNeighborsList(node)
         if(neighborsOfNeighbor.nonEmpty){
           neighborsOfNeighbor.foreach({entry =>
             if(!neighbors.contains(entry)){neighbors.append(entry)}
           })}
       })
       listOfNeighboringNodesForEachCuiOfAbbr.append(neighbors)
     })
   }
   (token,listOfCUIsForAbbr,listOfNeighboringNodesForEachCuiOfAbbr)
  }


  def computeSimiliartyForUnDisambiguated(tupleEntry: (String, ListBuffer[Long], ListBuffer[ListBuffer[Long]])) : Unit={

    val abbr = tupleEntry._1
    val listOfCuis = tupleEntry._2
    val listoFneighborsForCuis = tupleEntry._3
    val listOfContextWordCuis = listOfDisambiguatedNeighborCuis.value

    val result = Similarity.calculateSimilarity(abbr, listOfCuis, listoFneighborsForCuis, listOfContextWordCuis)

    mapOfDisambiguatedEntriesInLastStep.put(result._1, result._2)
  }
}