package keystoneml.workflow

/**
 * A mix-in that attaches a weight to an operator that represents how often it must iterate
 * over its input.
 */
trait WeightedOperator {
  val weight: Int
}
