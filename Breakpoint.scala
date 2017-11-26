import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.expressions.Aggregator

import coverage.PCT
import coverage.Coverage._

case class Span(
  ref_name: String,
  bc: String,
  beg: Int,
  end: Int,
  read_count: Int
)

object CalcBreakPoints extends Aggregator[Span, Coverage, Array[Int]] {
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

  def zero: Coverage = Coverage()

  def reduce(buffer: Coverage, span: Span): Coverage = {
    val cov = Coverage(PCT(span.beg - 1, 0, 1), PCT(span.end, 1, 0))
    mergeCoverages(buffer, cov)
  }

  def merge(cov1: Coverage, cov2: Coverage): Coverage = {
    mergeCoverages(cov1, cov2)
  }

  def finish(cov: Coverage): Array[Int] = {
    val cov_cutoff = 100;
    val bp = cov
      .filter(i => (i.cov >= cov_cutoff && i.nextCov < cov_cutoff) || (i.cov <= cov_cutoff && i.nextCov > cov_cutoff))
      .map(_.loc)
    bp
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

  def main(args: Array[String]) {
    val spanSchema = StructType(
      Array(
        StructField("ref_name", StringType, true),
        StructField("bc", StringType, true),
        StructField("beg", IntegerType, true),
        StructField("end", IntegerType, true),
        StructField("read_count", IntegerType, true)
      )
    )

    val spanFile = args(0)
    val output = args(1)

    val spark = SparkSession.builder
      .appName(s"Sparkle breakpoints for $spanFile")
      .master("local[*]")
      .enableHiveSupport()
      // .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate()
    import spark.implicits._

    // val ds = spark.read.option("sep", "\t").schema(spanSchema).csv(spanFile).as[Span]
    val ds = spark.read.schema(spanSchema).csv(spanFile).as[Span]

    val lineCount = ds.count
    println(s"# lines in $spanFile: $lineCount")
    val cbp = CalcBreakPoints.toColumn.name("bp_array")
    val res = ds.groupByKey(a => a.ref_name).agg(cbp)
    val colNames = Seq("ref_name", "break_point")
    val out = res.filter(_._2.length > 0).flatMap(i => i._2.map(j => (i._1, j))).toDF(colNames: _*)

    time {out.write.format("parquet").mode("overwrite").save(output)}

    spark.stop()
  
  }
}
