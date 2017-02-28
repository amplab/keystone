package keystoneml.workflow

/**
 * A mix-in that attaches a weight to a node that represents how often it must iterate
 * over its input.
 */
trait WeightedNode {
  val weight: Int
}
