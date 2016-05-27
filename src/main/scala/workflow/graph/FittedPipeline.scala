package workflow.graph

import org.apache.spark.rdd.RDD


class FittedPipeline[A, B] private[graph] (
    private[graph] val transformerGraph: TransformerGraph,
    private[graph] val source: SourceId,
    private[graph] val sink: SinkId
  ) extends Chainable[A, B] with Serializable {

  private[graph] override def toPipeline: Pipeline[A, B] = new Pipeline(
    new GraphExecutor(transformerGraph.toGraph, optimize = false),
    source,
    sink)

  /**
   * The application of this FittedPipeline to a single input item.
   *
   * @param in  The input item to pass into this transformer
   * @return  The output value
   */
  def apply(in: A): B = toPipeline.apply(in).get()

  /**
   * The application of this FittedPipeline to an RDD of input items.
   *
   * @param in The RDD input to pass into this transformer
   * @return The RDD output for the given input
   */
  def apply(in: RDD[A]): RDD[B] = toPipeline.apply(in).get()

}
