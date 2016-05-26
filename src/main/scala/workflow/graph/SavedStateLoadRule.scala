package workflow.graph

/**
 * A rule to load any saved state for the nodes w/ prefixes attached
 */
object SavedStateLoadRule extends Rule {
  override def apply(plan: Graph, prefixes: Map[NodeId, Prefix]): (Graph, Map[NodeId, Prefix]) = {
    val newGraph = prefixes.foldLeft(plan) {
      case (curGraph, (node, prefix)) =>
        Pipeline.state.get(prefix).map {
          case expression =>
            curGraph.setOperator(node, new ExpressionOperator(expression))
              .setDependencies(node, Seq())
        }.getOrElse(curGraph)
    }

    (newGraph, prefixes)
  }
}
