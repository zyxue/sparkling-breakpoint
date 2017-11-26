package coverage

// PCT: PointCoverageTransition
case class PCT(
  loc: Int,
  cov: Int,
  nextCov: Int
)

object Coverage {
  // Coverage is overloaded both as
  // 1. a data type and
  // 2. a function that constructs a value of Coverage type
  type Coverage = Array[PCT]
  def Coverage(xs: PCT*) = Array(xs: _*)
}
