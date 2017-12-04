package org.sparklingbreakpoint

import scala.collection.mutable.ArrayBuffer

import org.scalatest.FunSuite

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class BreakpointSuite extends FunSuite {
  def checkCovSum(cov1: ArrayBuffer[PCT], cov2: ArrayBuffer[PCT], expected: ArrayBuffer[PCT]): Unit = {
    val depthCutoff = 5
    val cc = new BreakpointCalculator(depthCutoff)
    assert(cc.consolidateCoverage(cov1 ++ cov2) === expected)
  }

  test("sum identical coverages") {
    checkCovSum(
      ArrayBuffer(PCT(-1, 0, 10), PCT(10, 10, 0)),
      ArrayBuffer(PCT(-1, 0, 10), PCT(10, 10, 0)),
      ArrayBuffer(PCT(-1,0,20), PCT(10,20,0))
    )
  }

  test("sum coverages with equivalent ranges but not the same depth") {
    checkCovSum(
      ArrayBuffer(PCT(-1, 0, 10), PCT(10, 10, 0)),
      ArrayBuffer(PCT(-1, 0, 5), PCT(10, 5, 0)),
      ArrayBuffer(PCT(-1, 0, 15), PCT(10, 15, 0))
    )
  }

  test("sum coverages not the same range") {
    checkCovSum(
      ArrayBuffer(PCT(-1, 0, 10), PCT(10, 10, 0)),
      ArrayBuffer(PCT(2, 0, 5), PCT(10, 5, 0)),
      ArrayBuffer(PCT(-1, 0, 10), PCT(2, 10, 15), PCT(10, 15, 0))
    )
  }

  test("sum coverages: one range within another") {
    checkCovSum(
      ArrayBuffer(PCT(-1, 0, 10), PCT(10, 10, 0)),
      ArrayBuffer(PCT(2, 0, 5), PCT(8, 5, 0)),
      ArrayBuffer(PCT(-1, 0, 10), PCT(2, 10, 15), PCT(8, 15, 10), PCT(10, 10, 0))
    )
  }

  test("sum coverages: one range intersects with another") {
    checkCovSum(
      ArrayBuffer(PCT(-1, 0, 10), PCT(10, 10, 0)),
      ArrayBuffer(PCT(2, 0, 5), PCT(12, 5, 0)),
      ArrayBuffer(PCT(-1, 0, 10), PCT(2, 10, 15), PCT(10, 15, 5), PCT(12, 5, 0))
    )
  }

  test("sum coverages with different numbers of PCTs") {
    checkCovSum(
      ArrayBuffer(PCT(-1, 0, 10), PCT(10, 10, 0)),
      ArrayBuffer(PCT(2, 0, 5), PCT(6, 5, 2), PCT(12, 2, 0)),
      ArrayBuffer(PCT(-1, 0, 10), PCT(2, 10, 15), PCT(6, 15, 12), PCT(10, 12, 2), PCT(12, 2, 0))
    )
  }

  test("sum coverages: First cov not starting from -1") {
    checkCovSum(
      ArrayBuffer(PCT(1, 0, 10), PCT(10, 10, 0)),
      ArrayBuffer(PCT(2, 0, 5), PCT(6, 5, 2), PCT(12, 2, 0)),
      ArrayBuffer(PCT(1, 0, 10), PCT(2, 10, 15), PCT(6, 15, 12), PCT(10, 12, 2), PCT(12, 2, 0))
    )
  }

  test("sum coverages with interleaving PCTs") {
    checkCovSum(
      ArrayBuffer(PCT(3, 0, 10), PCT(10, 10, 0)),
      ArrayBuffer(PCT(2, 0, 5), PCT(6, 5, 2), PCT(12, 2, 0)),
      ArrayBuffer(PCT(2, 0, 5), PCT(3, 5, 15), PCT(6, 15, 12), PCT(10, 12, 2), PCT(12, 2, 0))
    )
  }

  test("sum coverages with identical ranges not starting from -1") {
    checkCovSum(
      ArrayBuffer(PCT(3, 0, 10), PCT(10, 10, 0)),
      ArrayBuffer(PCT(3, 0, 5), PCT(10, 5, 0)),
      ArrayBuffer(PCT(3, 0, 15), PCT(10, 15, 0))
    )
  }

  def checkConsolidatedCoverage(cov: ArrayBuffer[PCT], expected: ArrayBuffer[PCT]): Unit = {
    val depthCutoff = 5
    val cc = new BreakpointCalculator(depthCutoff)
    assert(cc.consolidateCoverage(cov) === expected)
  }

  test("sum coverages at a single location") {
    checkConsolidatedCoverage(
      ArrayBuffer(PCT(-1, 0, 1), PCT(0, 1, 2), PCT(1, 2, 0)) ++ ArrayBuffer(PCT(-1, 0, 1), PCT(0, 1, 3), PCT(1, 3, 0)),
      ArrayBuffer(PCT(-1, 0, 2), PCT(0, 2, 5), PCT(1, 5, 0))
    )
  }


}
