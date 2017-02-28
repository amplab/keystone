package keystoneml.workflow

import keystoneml.nodes.util.Cacher

/**
 * Extract the prefixes of all Nodes whose state we want to save for reuse by other Pipeline apply and fit calls.
 * This is all nodes that either have a Cacher or an EstimatorOperator as the internal operator.
 */
object ExtractSaveablePrefixes extends Rule {
  override def apply(plan: Graph, prefixes: Map[NodeId, Prefix]): (Graph, Map[NodeId, Prefix]) = {
    val nodesToExtract = plan.operators.collect {
      case (node, _: Cacher[_]) => node
      case (node, _: EstimatorOperator) => node
    }

    val newPrefixes = nodesToExtract.map {
      node => (node, Prefix.findPrefix(plan, node))
    }.toMap

    (plan, newPrefixes)
  }
}
