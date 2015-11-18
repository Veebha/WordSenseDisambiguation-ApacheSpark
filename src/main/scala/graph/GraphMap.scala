package graph

import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object GraphMap {

  var map : Broadcast[mutable.HashMap[Long, ListBuffer[Long]]] = null

  def setGraphMap (argMap: Broadcast[mutable.HashMap[Long, ListBuffer[Long]]]) : Unit ={
    map = argMap
  }










}
