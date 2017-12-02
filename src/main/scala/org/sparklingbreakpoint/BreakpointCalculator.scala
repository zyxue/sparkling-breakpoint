package org.sparklingbreakpoint

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.expressions.Aggregator


// PCT: PointCoverageTransition
case class PCT(
  loc: Int,
  cov: Int,
  nextCov: Int
)


class BreakpointCalculator(depthCutoff: Int) extends Aggregator[ThinExtent, Array[PCT], Array[Int]] {
  def consolidateCoverage(cov: Array[PCT]): Array[PCT] = {
    val cc = cov.sortBy(_.loc)
    var newCov = Array[PCT]()
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

  def zero: Array[PCT] = Array[PCT]()

  def reduce(buffer: Array[PCT], extent: ThinExtent): Array[PCT] = {
    val cov = Array(PCT(extent.Start - 1, 0, 1), PCT(extent.End - 1, 1, 0))
    buffer ++ cov
  }

  def merge(cov1: Array[PCT], cov2: Array[PCT]): Array[PCT] = {
    cov1 ++ cov2
  }

  def finish(cov: Array[PCT]): Array[Int] = {
    val csCov = consolidateCoverage(cov)
    val bp = csCov
      .filter(i => (i.cov >= depthCutoff && i.nextCov < depthCutoff) || (i.cov < depthCutoff && i.nextCov >= depthCutoff))
      .map(_.loc)
    val len = bp.length
    bp.slice(1, len - 1) // removing beginning and ending breakpoints
  }

  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[Array[PCT]] = Encoders.kryo

  // Specifies the Encoder for the final output value type
  // def outputEncoder: Encoder[Int] = Encoders.scalaInt
  def outputEncoder: Encoder[Array[Int]] = Encoders.kryo
}
