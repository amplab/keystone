package workflow.graph

// This is serializable :)
private[graph] case class TransformerGraph(
  sources: Set[SourceId],
  sinkDependencies: Map[SinkId, NodeOrSourceId],
  operators: Map[NodeId, TransformerOperator],
  dependencies: Map[NodeId, Seq[NodeOrSourceId]]
) {

  private[graph] def toGraph: Graph = {
    Graph(
      sources = sources,
      sinkDependencies = sinkDependencies,
      operators = operators,
      dependencies = dependencies)
  }
}
