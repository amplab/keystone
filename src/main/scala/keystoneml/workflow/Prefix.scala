package keystoneml.workflow


private[workflow] object Prefix {
  /**
   * Given a graph and a node, output the prefix of the id.
   * Will error if provided a node with a source in the dependencies.
   *
   * @param graph The graph to use
   * @param node A node in the graph
   * @return The prefix of that id
   */
  def findPrefix(graph: Graph, node: NodeId): Prefix = {
    val rootOp = graph.getOperator(node)
    val deps = graph.getDependencies(node).map {
      case dep: NodeId => findPrefix(graph, dep)
      case dep: SourceId =>
        throw new IllegalArgumentException("May not get the prefix of a node with Sources in the dependencies.")
    }

    Prefix(rootOp, deps)
  }
}

/**
 * This case class represents the logical prefix of a node in a Pipeline.
 * @param operator The operator stored at the node
 * @param deps The prefixes of the operator's dependencies
 */
private[workflow] case class Prefix(operator: Operator, deps: Seq[Prefix])
