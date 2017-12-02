package org.sparklingbreakpoint

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.expressions.Aggregator

// import org.apache.spark.SparkContext
// import org.apache.spark.SparkConf

case class Extent(
  Rname: String,
  Start: Int,
  End: Int,
  Size: Int,
  BX: String,
  MI: Int,
  Reads: Int,
  Mapq_median: Float,
  AS_median: Float,
  NM_median: Float
)


//removed unnecessary columns
case class ThinExtent(
  Rname: String,
  Start: Int,
  End: Int
)


object Breakpoint {
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1e9 + "s")
    result
  }


  def main(args: Array[String]): Unit = {
    val extentSchema = StructType(
      Array(
        StructField("Rname", StringType, true),
        StructField("Start", IntegerType, true),
        StructField("End", IntegerType, true),
        StructField("Size", IntegerType, true),
        StructField("BX", StringType, true),
        StructField("MI", IntegerType, true),
        StructField("Reads", IntegerType, true),
        StructField("Mapq_median", FloatType, true),
        StructField("AS_median", FloatType, true),
        StructField("NM_median", FloatType, true)
      )
    )

    // val conf = new SparkConf().setAppName("lele").setMaster("local[*]")
    // val sc = new SparkContext(conf)

    val extentFile = args(0)
    val output = args(1)
    val depthCutoff = args(2).toInt
    val molSizeCutoff = args(3).toInt
    val maxMolExtent = args(4).toInt

    val spark = SparkSession.builder
      .appName("Sparkling-breakpoints")
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "655360")
      // .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate()
    import spark.implicits._
    // val ds = spark.read.schema(extentSchema).csv(extentFile).as[Extent]

    val df_extent = spark.read
      .option("sep", "\t")
      .option("header", true)
      .schema(extentSchema)
      .csv(extentFile)
      .as[Extent]

    val ndf_extent = df_extent.filter($"Size" >= molSizeCutoff)
    val colsToDrop = List("Size", "BX", "MI", "Reads", "Mapq_median", "AS_median", "NM_median")
    val odf_extent = ndf_extent.drop(colsToDrop: _*).as[ThinExtent]

    // val lineCount = ds.count
    // println(s"# lines in $extentFile: $lineCount")
    val cbp = new BreakpointCalculator(depthCutoff, maxMolExtent).toColumn.name("BreakpointArray")
    val res = odf_extent.groupByKey(i => i.Rname).agg(cbp)
    val colNames = Seq("Rname", "breakpoint")
    val out = res.filter(_._2.length > 0).flatMap(i => i._2.map(j => (i._1, j))).toDF(colNames: _*)

    time {out.write.format(output.split('.').last).save(output)}

    spark.stop()
  }
}
