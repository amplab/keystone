package workflow.graph

import org.apache.spark.rdd.RDD

/**
 * This class is a lazy wrapper around the output of a pipeline that was passed an RDD as input.
 *
 * Under the hood, it extends [[PipelineResult]] and keeps track of the necessary execution plan.
 */
class PipelineDatasetOut[T] private[graph] (executor: GraphExecutor, sink: SinkId)
  extends PipelineResult[RDD[T]](
    executor,
    sink)

object PipelineDatasetOut {
  private[graph] def apply[T](rdd: RDD[T]): PipelineDatasetOut[T] = {
    val emptyGraph = Graph(Set(), Map(), Map(), Map())
    val (graphWithDataset, nodeId) = emptyGraph.addNode(new DatasetOperator(rdd), Seq())
    val (graph, sinkId) = graphWithDataset.addSink(nodeId)

    new PipelineDatasetOut[T](new GraphExecutor(graph), sinkId)
  }
}
