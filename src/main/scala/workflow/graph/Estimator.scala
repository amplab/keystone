package workflow.graph

import org.apache.spark.rdd.RDD

/**
 * An estimator has a `fitRDD` method which takes an input and emits a [[Transformer]]
 * @tparam A The type of input this estimator (and the resulting Transformer) takes
 * @tparam B The output type of the Transformer this estimator produces when being fit
 */
abstract class Estimator[A, B] extends EstimatorOperator {
  /**
   * Constructs a pipeline that fits this estimator to training data,
   * then applies the resultant transformer to the Pipeline input.
   *
   * @param data The training data
   * @return A pipeline that fits this estimator and applies the result to inputs.
   */
  final def withData(data: RDD[A]): Pipeline[A, B] = {
    withData(PipelineDataset(data))
  }

  /**
   * Constructs a pipeline that fits this estimator to training data,
   * then applies the resultant transformer to the Pipeline input.
   *
   * @param data The training data
   * @return A pipeline that fits this estimator and applies the result to inputs.
   */
  final def withData(data: PipelineDataset[A]): Pipeline[A, B] = {
    // Remove the data sink,
    // Then insert this estimator into the graph with the data as the input
    val curSink = data.executor.graph.getSinkDependency(data.sink)
    val (estGraph, estId) = data.executor.graph.removeSink(data.sink).addNode(this, Seq(curSink))

    // Now that the estimator is attached to the data, we need to build a pipeline DAG
    // that applies the fit output of the estimator. We do this by creating a new Source in the DAG,
    // Adding a delegating transformer that depends on the source and the label estimator,
    // And finally adding a sink that connects to the delegating transformer.
    val (estGraphWithNewSource, sourceId) = estGraph.addSource()
    val (almostFinalGraph, delegatingId) = estGraphWithNewSource.addNode(new DelegatingOperator, Seq(estId, sourceId))
    val (newGraph, sinkId) = almostFinalGraph.addSink(delegatingId)

    new Pipeline(new GraphExecutor(newGraph), sourceId, sinkId)
  }

  /**
   * The non-type-safe `fitRDDs` method of [[EstimatorOperator]] that is being overridden by the Estimator API.
   */
  final override private[graph] def fitRDDs(inputs: Seq[DatasetExpression]): TransformerOperator = {
    fit(inputs.head.get.asInstanceOf[RDD[A]])
  }

  /**
   * The type-safe method that ML developers need to implement when writing new Estimators.
   *
   * @param data The estimator's training data.
   * @return A new transformer
   */
  def fit(data: RDD[A]): Transformer[A, B]
}
