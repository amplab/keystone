package workflow.graph

import org.apache.spark.rdd.RDD

// A lazy representation of a pipeline output
class PipelineDatasetOut[T] private[graph] (executor: GraphExecutor, sink: SinkId, source: Option[(SourceId, RDD[_])])
  extends GraphExecution(
    executor,
    source.map(sourceAndVal => Map(sourceAndVal._1 -> DatasetOperator(sourceAndVal._2))).getOrElse(Map()),
    sink,
    _.asInstanceOf[DatasetExpression].get.asInstanceOf[RDD[T]])

object PipelineDatasetOut {
  private[graph] def apply[T](rdd: RDD[T]): PipelineDatasetOut[T] = {
    val emptyGraph = Graph(Set(), Map(), Map(), Map())
    val (graphWithDataset, nodeId) = emptyGraph.addNode(new DatasetOperator(rdd), Seq())
    val (graph, sinkId) = graphWithDataset.addSink(nodeId)

    new PipelineDatasetOut[T](new GraphExecutor(graph, Map()), sinkId, None)
  }
}
