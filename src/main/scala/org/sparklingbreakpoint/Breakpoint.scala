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

// PCT: PointCoverageTransition
case class PCT(
  loc: Int,
  cov: Int,
  nextCov: Int
)

class BreakpointCalculator(depthCutoff: Int) extends Aggregator[Extent, Array[PCT], Array[Int]] {
  // Coverage is overloaded both as
  // 1. a data type and
  // 2. a function that constructs a value of Coverage type
  type Coverage = Array[PCT]

  def Coverage(xs: PCT*) = Array(xs: _*)

  def mergeCoverages(cov1: Coverage, cov2: Coverage): Coverage = {
    if (cov1.length == 0) {
      cov2
    } else if (cov2.length == 0) {
      cov1
    } else {
      val cc = (cov1 ++ cov2).sortBy(_.loc)
      var newCov = Coverage()
      for (pct <- cc) {
        if (newCov.length == 0) {
          newCov :+= pct
        } else {
          var ti = newCov.length - 1 // tail index
          var tailPCT = newCov(ti)

          if (pct.loc == tailPCT.loc) {
            newCov(ti) = PCT(pct.loc,
                             tailPCT.cov,
                             tailPCT.nextCov + pct.nextCov - pct.cov)
          } else {
            newCov :+= PCT(pct.loc,
                           tailPCT.nextCov,
                           tailPCT.nextCov + pct.nextCov - pct.cov)
          }
        }
      }
      newCov
    }
  }

  // def mergeCoverages(cov1: Coverage, cov2: Coverage): Coverage = {
  //   if (cov1.length == 0) {
  //     cov2
  //   } else if (cov2.length == 0) {
  //     cov1
  //   } else {
  //     var i = 0
  //     var j = 0
  //     var len1 = cov1.length
  //     var len2 = cov2.length
  //     var newCov = Coverage()
  //     while (i < len1 || j < len2) {
  //       // TO refactor, determine the next PCT to process
  //       var pct = PCT(0, 0, 0)
  //       if (i < len1 && j < len2) {
  //         var pcti = cov1(i)
  //         var pctj = cov2(j)

  //         if (pctj.loc >= pcti.loc) {
  //           pct = pcti
  //           i += 1
  //         } else {
  //           pct = pctj
  //           j += 1
  //         }
  //       } else if (i < len1) {
  //         pct = cov1(i)
  //         i += 1
  //       } else {
  //         pct = cov2(j)
  //         j += 1
  //       }

  //       // integrate the pct into newCov
  //       if (newCov.length == 0) {
  //         newCov :+= pct
  //       } else {
  //         var ti = newCov.length - 1 // tail index
  //         var tailPCT = newCov(ti)

  //         if (pct.loc == tailPCT.loc) {
  //           newCov(ti) = PCT(pct.loc,
  //                            tailPCT.cov,
  //                            tailPCT.nextCov + pct.nextCov - pct.cov)
  //         } else {
  //           newCov :+= PCT(pct.loc,
  //                          tailPCT.nextCov,
  //                          tailPCT.nextCov + pct.nextCov - pct.cov)
  //         }
  //       }
  //     }
  //     newCov
  //   }
  // }

  def zero: Coverage = Coverage()

  def reduce(buffer: Coverage, extent: Extent): Coverage = {
    val cov = Coverage(PCT(extent.Start - 1, 0, 1), PCT(extent.End, 1, 0))
    mergeCoverages(buffer, cov)
  }

  def merge(cov1: Coverage, cov2: Coverage): Coverage = {
    mergeCoverages(cov1, cov2)
  }

  def finish(cov: Coverage): Array[Int] = {
    val bp = cov
      .filter(i => (i.cov >= depthCutoff && i.nextCov < depthCutoff) || (i.cov < depthCutoff && i.nextCov >= depthCutoff))
      .map(_.loc)
    val len = bp.length
    bp.slice(1, len - 1) // removing beginning and ending breakpoints
  }

  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[Coverage] = Encoders.kryo

  // Specifies the Encoder for the final output value type
  // def outputEncoder: Encoder[Int] = Encoders.scalaInt
  def outputEncoder: Encoder[Array[Int]] = Encoders.kryo
}

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

    val spark = SparkSession.builder
      .appName(s"Sparkle breakpoints for $extentFile")
      .master("local[*]")
      .enableHiveSupport()
      // .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate()
    import spark.implicits._
    // val ds = spark.read.schema(extentSchema).csv(extentFile).as[Extent]

    val ds = spark.read
      .option("sep", "\t")
      .option("header", true)
      .schema(extentSchema)
      .csv(extentFile)
      .as[Extent]

    // val lineCount = ds.count
    // println(s"# lines in $extentFile: $lineCount")
    val cbp = new BreakpointCalculator(depthCutoff).toColumn.name("bp_array")
    val res = ds.groupByKey(i => i.Rname).agg(cbp)
    val colNames = Seq("Rname", "break_point")
    val out = res.filter(_._2.length > 0).flatMap(i => i._2.map(j => (i._1, j))).toDF(colNames: _*)

    time {out.write.format("parquet").save(output)}

    spark.stop()
  }
}
