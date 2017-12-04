package org.sparklingbreakpoint

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.expressions.Aggregator


// PCT: PointCoverageTransition
case class PCT(
  loc: Int,
  cov: Int,
  nextCov: Int
)


class BreakpointCalculator(depthCutoff:Int=50, maxMolExtent:Int = 50000) extends Aggregator[ThinExtent, ArrayBuffer[PCT], ArrayBuffer[Int]] {
  def consolidateCoverage(cov: ArrayBuffer[PCT]): ArrayBuffer[PCT] = {
    val cc = cov.sortBy(_.loc)
    var newCov = ArrayBuffer[PCT]()
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

  def zero: ArrayBuffer[PCT] = ArrayBuffer[PCT]()

  def reduce(buffer: ArrayBuffer[PCT], extent: ThinExtent): ArrayBuffer[PCT] = {
    val cov = ArrayBuffer(PCT(extent.Start - 1, 0, 1), PCT(extent.End - 1, 1, 0))
    buffer ++ cov
  }

  def merge(cov1: ArrayBuffer[PCT], cov2: ArrayBuffer[PCT]): ArrayBuffer[PCT] = {
    cov1 ++ cov2
  }

  def finish(cov: ArrayBuffer[PCT]): ArrayBuffer[Int] = {
    val csCov = consolidateCoverage(cov)
    val bp = csCov
      .filter(i => (i.cov >= depthCutoff && i.nextCov < depthCutoff) || (i.cov < depthCutoff && i.nextCov >= depthCutoff))
    val len = bp.length

    // removing beginning and ending breakpoints
    val bp2 = bp.slice(1, len - 1)

    // remove pair breakpoints that are less than maxMolExtent away
    val bpi = bp2.zipWithIndex
    val bpOdd:ArrayBuffer[PCT] = bpi.filter(_._2 % 2 == 0).map(_._1) // 0-based: so it's actually Odd element
    val bpEven:ArrayBuffer[PCT] = bpi.filter(_._2 % 2 == 1).map(_._1)
    val bpz = bpOdd.zip(bpEven)
      .filter(i => (i._2.loc - i._1.loc < maxMolExtent))
      .flatMap(pcts => ArrayBuffer(pcts._1, pcts._2))
    bpz.map(_.loc)
  }

  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[ArrayBuffer[PCT]] = Encoders.kryo

  // Specifies the Encoder for the final output value type
  // def outputEncoder: Encoder[Int] = Encoders.scalaInt
  def outputEncoder: Encoder[ArrayBuffer[Int]] = Encoders.kryo
}
