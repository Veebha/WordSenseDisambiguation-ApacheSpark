package metrics

object Metrics {

  var ApplicationStartTime: Long = 0
  var ApplicationEndTime: Long  = 0
  var GraphLoadingStartTime: Long  = 0
  var GraphLoadingEndTime: Long  = 0
  var SparkEngineStartTime: Long  = 0
  var SparkEngineEndTIme: Long  = 0
  var SprakGraphScanStart: Long =0
  var SprakGraphScanStop: Long =0


  def showMetrics() ={

    val secondsToCompleteApplication = (ApplicationEndTime - ApplicationStartTime) / 1000
    val secondsToSparkEngineProcessing = (SparkEngineEndTIme - SparkEngineStartTime) / 1000
    val secondsToGrapLoading = (GraphLoadingEndTime - GraphLoadingStartTime) / 1000
    val secondsToGraphScanValues = (SprakGraphScanStop - SprakGraphScanStart) / 1000

   println("\n\n\t\t\t\t################################# METRICS ##########################################\t\t\n\n")

    println("\n\t\t\t\t\tTotal Application Run Time => " + secondsToCompleteApplication + "\t\t\t" + (ApplicationEndTime - ApplicationStartTime) + " Millis" )
    println("\n\t\t\t\t\tTotal Graph Loading Time => " + secondsToGrapLoading  + "\t\t\t" + (SparkEngineEndTIme - SparkEngineStartTime) + " Millis" )
    println("\n\t\t\t\t\tTotal GraphScan Time => " + secondsToGraphScanValues +"\t\t\t" + (GraphLoadingEndTime - GraphLoadingStartTime) + " Millis" )
    println("\n\t\t\t\t\tTotal SparkEngine Run Time => " + secondsToSparkEngineProcessing +"\t\t\t" + (SprakGraphScanStop - SprakGraphScanStart) + " Millis" )

    println("\n\n\t\t\t\t################################# METRICS ##########################################\t\t\n\n")
  }





}
