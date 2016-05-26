package workflow.graph

import org.apache.spark.rdd.RDD

/**
 * A LabelEstimator has a `fitRDDs` method which takes input data and input labels,
 * and emits a [[Transformer]].
 *
 * @tparam A The type of input data this estimator (and the resulting Transformer) takes
 * @tparam B The output type of the Transformer this estimator produces when being fit
 * @tparam L The type of input labels this estimator takes at training time
 */
abstract class LabelEstimator[A, B, L] extends EstimatorOperator {
  /**
   * Constructs a pipeline that fits this label estimator to training data and labels,
   * then applies the resultant transformer to the Pipeline input.
   *
   * @param data The training data
   * @param labels The training labels
   * @return A pipeline that fits this label estimator and applies the result to inputs.
   */
  final def fit(data: RDD[A], labels: PipelineDatasetOut[L]): Pipeline[A, B] = {
    fit(PipelineDatasetOut(data), labels)
  }

  /**
   * Constructs a pipeline that fits this label estimator to training data and labels,
   * then applies the resultant transformer to the Pipeline input.
   *
   * @param data The training data
   * @param labels The training labels
   * @return A pipeline that fits this label estimator and applies the result to inputs.
   */
  final def fit(data: PipelineDatasetOut[A], labels: RDD[L]): Pipeline[A, B] = {
    fit(data, PipelineDatasetOut(labels))
  }

  /**
   * Constructs a pipeline that fits this label estimator to training data and labels,
   * then applies the resultant transformer to the Pipeline input.
   *
   * @param data The training data
   * @param labels The training labels
   * @return A pipeline that fits this label estimator and applies the result to inputs.
   */
  final def fit(data: RDD[A], labels: RDD[L]): Pipeline[A, B] = {
    fit(PipelineDatasetOut(data), PipelineDatasetOut(labels))
  }

  /**
   * Constructs a pipeline that fits this label estimator to training data and labels,
   * then applies the resultant transformer to the Pipeline input.
   *
   * @param data The training data
   * @param labels The training labels
   * @return A pipeline that fits this label estimator and applies the result to inputs.
   */
  final def fit(data: PipelineDatasetOut[A], labels: PipelineDatasetOut[L]): Pipeline[A, B] = {
    // Add the data input and the labels inputs into the same Graph
    val (dataAndLabels, _, _, labelSinkMapping) =
      data.getGraph.addGraph(labels.getGraph)

    // Remove the data sink & the labels sink,
    // Then insert this label estimator into the graph with the data & labels as the inputs
    val dataSink = dataAndLabels.getSinkDependency(data.getSink)
    val labelsSink = dataAndLabels.getSinkDependency(labelSinkMapping(labels.getSink))
    val (estimatorWithInputs, estId) = dataAndLabels
      .removeSink(data.getSink)
      .removeSink(labelSinkMapping(labels.getSink))
      .addNode(this, Seq(dataSink, labelsSink))

    // Now that the labeled estimator is attached to the data & labels, we need to build a pipeline DAG
    // that applies the fit output of the estimator. We do this by creating a new Source in the DAG,
    // Adding a delegating transformer that depends on the source and the label estimator,
    // And finally adding a sink that connects to the delegating transformer.
    val (estGraphWithNewSource, sourceId) = estimatorWithInputs.addSource()
    val (almostFinalGraph, delegatingId) = estGraphWithNewSource.addNode(new DelegatingOperator, Seq(estId, sourceId))
    val (newGraph, sinkId) = almostFinalGraph.addSink(delegatingId)

    // Finally, we construct a new pipeline w/ the new graph & new state.
    new ConcretePipeline(new GraphExecutor(newGraph), sourceId, sinkId)
  }

  /**
   * The non-type-safe `fitRDDs` method of [[EstimatorOperator]] that is being overridden by the LabelEstimator API.
   */
  final override private[graph] def fitRDDs(inputs: Seq[DatasetExpression]): TransformerOperator = {
    fitRDDs(inputs(0).get.asInstanceOf[RDD[A]], inputs(1).get.asInstanceOf[RDD[L]])
  }

  /**
   * The type-safe method that ML developers need to implement when writing new Estimators.
   *
   * @param data The estimator's training data.
   * @param labels The estimator's training labels
   * @return A new transformer
   */
  protected def fitRDDs(data: RDD[A], labels: RDD[L]): Transformer[A, B]

}
