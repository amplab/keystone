package workflow.graph


private[graph] object Prefix {
  /**
   * Given a graph and a node, output the prefix of the id.
   * Will error if provided a node with a source in the dependencies.
   *
   * @param graph The graph to use
   * @param node A node in the graph
   * @return The prefix of that id
   */
  def get(graph: Graph, node: NodeId): Prefix = {
    val rootOp = graph.getOperator(node)
    val deps = graph.getDependencies(node).map {
      case dep: NodeId => get(graph, dep)
      case dep: SourceId =>
        throw new IllegalArgumentException("May not get the prefix of a node with Sources in the dependencies.")
    }

    Prefix(rootOp, deps)
  }
}

private[graph] case class Prefix(operator: Operator, deps: Seq[Prefix])
