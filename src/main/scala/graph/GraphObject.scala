package graph

import fileio.AppData
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object GraphObject {

  var graph: Graph[Int, String] = null

  def loadGraph(sc: SparkContext, appData: AppData): Boolean = {

    type Relation = String
    val graphData = appData.loadObjectFromS3ToSparkRDD(sc)

    val edges: RDD[Edge[Relation]] = graphData.map { line =>
      val row = line.split("\t")
      Edge(replaceChar(row(0)), replaceChar(row(1)), row(2))
    }
    graph = Graph.fromEdges(edges, defaultValue = 1)
    graph.cache()
    true
  }

  def getNeighborsList(cui: Long): ListBuffer[Long] = {

    val map = new mutable.HashMap[Long,Byte]
    val listOfNeighboringCuis = new ListBuffer[Long]
    val tripletEntries = graph.triplets.filter(triplet => triplet.srcId == cui).collect()
    tripletEntries.foreach({ entry =>
      map.put(entry.dstId,0)
    })
    map.keySet.toList.foreach(listOfNeighboringCuis.append(_))
    listOfNeighboringCuis
  }


  def getNeighborsAndRelations(cui: String): ListBuffer[(Long,String)] = {
    val list = new ListBuffer[(Long,String)]
    val tripletEntries = graph.triplets.filter(triplet => triplet.srcId == replaceChar(cui)).collect()
    tripletEntries.foreach({ entry =>
      list += Tuple2(entry.dstId, entry.attr)
    })
    list
  }

  def replaceChar(cui: String): Long = {
    val numericContent = cui.replace('C', '0')
    numericContent.toLong
  }

  def printGraphInfo(): Unit ={
    println("Number of vertices => " + graph.numVertices)
    println("Number of edges => " + graph.numEdges)
  }

  def getNeighborsForAbbrAndContextCuis(listOfCuis: List[ListBuffer[(String, ListBuffer[Long], ListBuffer[Long])]]) : mutable.HashMap[Long,ListBuffer[Long]]  = {

    val mapOfCuisToGetNeighbors = new mutable.HashMap[Long,ListBuffer[Long]]

    val uniqueCuis = new  mutable.HashMap[Long,Byte]

    listOfCuis.foreach({listEntry =>
      listEntry.foreach({tupleEntry =>
        val abbrCuis = tupleEntry._2
        val contextcuis = tupleEntry._3
        if(abbrCuis != null){abbrCuis.foreach(uniqueCuis.put(_,0))}
        if(contextcuis != null){contextcuis.foreach(uniqueCuis.put(_,0))}
      })
    })

    uniqueCuis.foreach({cui =>
      val listOfNeighbors = getNeighborsList(cui._1)
      if (listOfNeighbors.nonEmpty) {
        mapOfCuisToGetNeighbors.put(cui._1, listOfNeighbors)
      }
    })

    mapOfCuisToGetNeighbors
  }

  def getNeighborsForAllCuisInAList(listOfCuis: ListBuffer[Long]) : ListBuffer[Long]  = {

    val fullListOfNeighbors = new ListBuffer[Long]


    listOfCuis.foreach({cui =>
      val listOfNeighbors = getNeighborsList(cui)
      if (listOfNeighbors.nonEmpty) {
        listOfNeighbors.foreach({neighborCui =>
          if(!fullListOfNeighbors.contains(neighborCui))
            {fullListOfNeighbors.append(neighborCui)}
          })
        }
      })

    fullListOfNeighbors
  }








  }
