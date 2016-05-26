package workflow.graph

/**
 * Extract the prefixes whose state we want to save (aka for any cacher or estimator node)
 */
object ExtractSaveablePrefixes extends Rule {
  override def apply(plan: Graph, prefixes: Map[NodeId, Prefix]): (Graph, Map[NodeId, Prefix]) = {
    val nodesToExtract = plan.operators.collect {
      case (node, _: Cacher[_]) => node
      case (node, _: EstimatorOperator) => node
    }

    val newPrefixes = nodesToExtract.map {
      node => (node, Prefix.get(plan, node))
    }.toMap

    (plan, newPrefixes)
  }
}
