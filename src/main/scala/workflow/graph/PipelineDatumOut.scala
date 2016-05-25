package workflow.graph

// A lazy representation of a pipeline output
class PipelineDatumOut[T] private[graph] (executor: GraphExecutor, sink: SinkId, source: Option[(SourceId, Any)])
  extends GraphExecution(
    executor,
    source.map(sourceAndVal => Map(sourceAndVal._1 -> DatumOperator(sourceAndVal._2))).getOrElse(Map()),
    sink,
    _.asInstanceOf[DatumExpression].get.asInstanceOf[T])

object PipelineDatumOut {
  private[graph] def apply[T](datum: T): PipelineDatumOut[T] = {
    val emptyGraph = Graph(Set(), Map(), Map(), Map())
    val (graphWithDataset, nodeId) = emptyGraph.addNode(new DatumOperator(datum), Seq())
    val (graph, sinkId) = graphWithDataset.addSink(nodeId)

    new PipelineDatumOut[T](new GraphExecutor(graph, Map()), sinkId, None)
  }
}
