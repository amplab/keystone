package workflow.graph

/**
 * Extract the prefixes whose state we want to save (aka for any cacher or estimator node)
 */
object ExtractSaveablePrefixes extends Rule {
  override def apply(plan: Graph, prefixes: Map[NodeId, Prefix]): (Graph, Map[NodeId, Prefix]) = {
    // TODO FIXME
    (plan, prefixes)
  }
}
