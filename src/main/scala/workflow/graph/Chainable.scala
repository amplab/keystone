package workflow.graph

import org.apache.spark.rdd.RDD

/**
 * TODO Write me :(
 * @tparam A
 * @tparam B
 */
trait Chainable[A, B] {
  private[graph] def toPipeline: Pipeline[A, B]

  /**
   * Chains a pipeline onto the end of this one, producing a new pipeline.
   * If either this pipeline or the following has already been executed, it will not need to be fit again.
   *
   * @param next the pipeline to chain
   */
  final def andThen[C](next: Chainable[B, C]): Pipeline[A, C] = {
    val nextPipe = next.toPipeline
    val (newGraph, _, _, sinkMapping) =
      toPipeline.executor.graph.connectGraph(nextPipe.executor.graph, Map(nextPipe.source -> toPipeline.sink))

    new Pipeline(new GraphExecutor(newGraph), toPipeline.source, sinkMapping(nextPipe.sink))
  }

  /**
   * Chains an estimator onto the end of this pipeline, producing a new pipeline.
   * If this pipeline has already been executed, it will not need to be fit again.
   *
   * @param est The estimator to chain onto the end of this pipeline
   * @param data The training data to use
   *             (the estimator will be fit on the result of passing this data through the current pipeline)
   */
  final def andThen[C](est: Estimator[B, C], data: RDD[A]): Pipeline[A, C] = {
    this andThen est.withData(toPipeline.apply(data))
  }

  /**
   * Chains an estimator onto the end of this pipeline, producing a new pipeline.
   * If this pipeline has already been executed, it will not need to be fit again.
   *
   * @param est The estimator to chain onto the end of this pipeline
   * @param data The training data to use
   *             (the estimator will be fit on the result of passing this data through the current pipeline)
   */
  final def andThen[C](est: Estimator[B, C], data: PipelineDatasetOut[A]): Pipeline[A, C] = {
    this andThen est.withData(toPipeline.apply(data))
  }

  /**
   * Chains a label estimator onto the end of this pipeline, producing a new pipeline.
   * If this pipeline has already been executed, it will not need to be fit again.
   *
   * @param est The estimator to chain onto the end of this pipeline
   * @param data The training data to use
   *             (the estimator will be fit on the result of passing this data through the current pipeline)
   * @param labels The labels to use when fitting the LabelEstimator. Must be zippable with the training data.
   */
  final def andThen[C, L](
    est: LabelEstimator[B, C, L],
    data: RDD[A],
    labels: RDD[L]
  ): Pipeline[A, C] = {
    this andThen est.withData(toPipeline.apply(data), labels)
  }

  /**
   * Chains a label estimator onto the end of this pipeline, producing a new pipeline.
   * If this pipeline has already been executed, it will not need to be fit again.
   *
   * @param est The estimator to chain onto the end of this pipeline
   * @param data The training data to use
   *             (the estimator will be fit on the result of passing this data through the current pipeline)
   * @param labels The labels to use when fitting the LabelEstimator. Must be zippable with the training data.
   */
  final def andThen[C, L](
    est: LabelEstimator[B, C, L],
    data: PipelineDatasetOut[A],
    labels: RDD[L]
  ): Pipeline[A, C] = {
    this andThen est.withData(toPipeline.apply(data), labels)
  }

  /**
   * Chains a label estimator onto the end of this pipeline, producing a new pipeline.
   * If this pipeline has already been executed, it will not need to be fit again.
   *
   * @param est The estimator to chain onto the end of this pipeline
   * @param data The training data to use
   *             (the estimator will be fit on the result of passing this data through the current pipeline)
   * @param labels The labels to use when fitting the LabelEstimator. Must be zippable with the training data.
   */
  final def andThen[C, L](
    est: LabelEstimator[B, C, L],
    data: RDD[A],
    labels: PipelineDatasetOut[L]
  ): Pipeline[A, C] = {
    this andThen est.withData(toPipeline.apply(data), labels)
  }

  /**
   * Chains a label estimator onto the end of this pipeline, producing a new pipeline.
   * If this pipeline has already been executed, it will not need to be fit again.
   *
   * @param est The estimator to chain onto the end of this pipeline
   * @param data The training data to use
   *             (the estimator will be fit on the result of passing this data through the current pipeline)
   * @param labels The labels to use when fitting the LabelEstimator. Must be zippable with the training data.
   */
  final def andThen[C, L](
    est: LabelEstimator[B, C, L],
    data: PipelineDatasetOut[A],
    labels: PipelineDatasetOut[L]
  ): Pipeline[A, C] = {
    this andThen est.withData(toPipeline.apply(data), labels)
  }

}
