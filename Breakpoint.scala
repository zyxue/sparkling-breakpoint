/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession

object Breakpoint {
  def main(args: Array[String]) {
    val logFile = args(0)
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs, from $logFile")
    spark.stop()
  
  }
}
