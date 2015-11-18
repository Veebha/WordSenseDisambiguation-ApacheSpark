package parsers

import scala.collection.mutable.ListBuffer

object MetaMapParser {

  def parse(line: String): (String, ListBuffer[Long]) = {

    var cuiList = new ListBuffer[Long]
    val tokens = line.split("=")
    val word = tokens(0)
    val cuiString = tokens(1).replace("|", ",")
    val resultList = cuiString.split(",")

    resultList.foreach({ item =>
      cuiList.append(item.toLong)
    })

    if (cuiList.isEmpty) {
      cuiList = null
    }
    (word, cuiList)
  }
}
