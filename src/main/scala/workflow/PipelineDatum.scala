package workflow

/**
 * This class is a lazy wrapper around the output of a pipeline that was passed a single datum as input.
 *
 * Under the hood, it extends [[PipelineResult]] and keeps track of the necessary execution plan.
 */
class PipelineDatum[T] private[workflow](executor: GraphExecutor, sink: SinkId)
  extends PipelineResult[T](
    executor,
    sink)

object PipelineDatum {
  private[workflow] def apply[T](datum: T): PipelineDatum[T] = {
    val emptyGraph = Graph(Set(), Map(), Map(), Map())
    val (graphWithDataset, nodeId) = emptyGraph.addNode(new DatumOperator(datum), Seq())
    val (graph, sinkId) = graphWithDataset.addSink(nodeId)

    new PipelineDatum[T](new GraphExecutor(graph), sinkId)
  }
}
