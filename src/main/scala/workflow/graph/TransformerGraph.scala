package workflow.graph

/**
 * TransformerGraphs are similar to [[Graph]]s, but unlike normal Graphs they may only contain
 * [[TransformerOperator]]s as operators, and as a result are guaranteed to be serializable.
 *
 * @param sources  The set of all [[SourceId]]s of sources in the graph
 * @param sinkDependencies  A map of [[SinkId]] to the id of the node or source the sink depends on
 * @param operators  A map of [[NodeId]] to the operator contained within that node
 * @param dependencies  A map of [[NodeId]] to the node's ordered dependencies
 */
private[graph] case class TransformerGraph(
  sources: Set[SourceId],
  sinkDependencies: Map[SinkId, NodeOrSourceId],
  operators: Map[NodeId, TransformerOperator],
  dependencies: Map[NodeId, Seq[NodeOrSourceId]]
) {

  /**
   * Convert this TransformerGraph into a standard [[Graph]]
   */
  private[graph] def toGraph: Graph = {
    Graph(
      sources = sources,
      sinkDependencies = sinkDependencies,
      operators = operators,
      dependencies = dependencies)
  }
}
