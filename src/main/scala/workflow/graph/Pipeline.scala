package workflow.graph

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * A Pipeline takes data as input (single item or an RDD), and outputs some transformation
 * of that data. Internally, a Pipeline contains a [[GraphExecutor]], a specified source, and a specified sink.
 * When a pipeline is applied to data it produces a [[PipelineResult]], in the form of either a [[PipelineDatasetOut]]
 * or a [[PipelineDatumOut]]. These are lazy wrappers around the scheduled execution under the hood,
 * and when their values are accessed the underlying [[Graph]] will be executed.
 *
 * Warning: Not thread-safe!
 *
 * @tparam A type of the data this Pipeline expects as input
 * @tparam B type of the data this Pipeline outputs
 */
trait Pipeline[A, B] {
  private[graph] val source: SourceId
  private[graph] val sink: SinkId
  private[graph] def executor: GraphExecutor

  /**
   * Lazily apply the pipeline to a single datum.
   *
   * @return A lazy wrapper around the result of passing the datum through the pipeline.
   */
  final def apply(datum: A): PipelineDatumOut[B] = {
    apply(PipelineDatumOut(datum))
  }

  /**
   * Lazily apply the pipeline to a dataset.
   *
   * @return A lazy wrapper around the result of passing the dataset through the pipeline.
   */
  final def apply(data: RDD[A]): PipelineDatasetOut[B] = {
    apply(PipelineDatasetOut(data))
  }

  /**
   * Lazily apply the pipeline to the lazy output of a different pipeline given an initial dataset.
   * If the previous pipeline has already been fit, it will not need to be fit again.
   *
   * @return A lazy wrapper around the result of passing lazy output from a different pipeline through this pipeline.
   */
  final def apply(data: PipelineDatasetOut[A]): PipelineDatasetOut[B] = {
    val (newGraph, _, _, sinkMapping) =
      data.getGraph.connectGraph(executor.graph, Map(source -> data.getSink))

    new PipelineDatasetOut[B](new GraphExecutor(newGraph), sinkMapping(sink))
  }

  /**
   * Lazily apply the pipeline to the lazy output of a different pipeline given an initial datum.
   * If the previous pipeline has already been fit, it will not need to be fit again.
   *
   * @return A lazy wrapper around the result of passing lazy output from a different pipeline through this pipeline.
   */
  final def apply(datum: PipelineDatumOut[A]): PipelineDatumOut[B] = {
    val (newGraph, _, _, sinkMapping) =
      datum.getGraph.connectGraph(executor.graph, Map(source -> datum.getSink))

    new PipelineDatumOut[B](new GraphExecutor(newGraph), sinkMapping(sink))
  }

  /**
   * Chains a pipeline onto the end of this one, producing a new pipeline.
   * If either this pipeline or the following has already been executed, it will not need to be fit again.
   *
   * @param next the pipeline to chain
   */
  final def andThen[C](next: Pipeline[B, C]): Pipeline[A, C] = {
    val (newGraph, _, _, sinkMapping) =
      executor.graph.connectGraph(next.executor.graph, Map(next.source -> sink))

    new ConcretePipeline(new GraphExecutor(newGraph), source, sinkMapping(next.sink))
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
    this andThen est.fit(apply(data))
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
    this andThen est.fit(apply(data))
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
    this andThen est.fit(apply(data), labels)
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
    this andThen est.fit(apply(data), labels)
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
    this andThen est.fit(apply(data), labels)
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
    this andThen est.fit(apply(data), labels)
  }

}

object Pipeline {
  // not threadsafe
  private[graph] val state: scala.collection.mutable.Map[Prefix, Expression] = scala.collection.mutable.Map()

  /**
   * The internally stored optimizer globally used for all Pipeline execution. Accessible using getter and setter.
   */
  private var _optimizer: Optimizer = DefaultOptimizer

  /**
   * @return The current global optimizer used during Pipeline execution.
   */
  def getOptimizer: Optimizer = _optimizer

  /**
   * Globally set a new optimizer to use during Pipeline execution.
   *
   * @param optimizer The new optimizer to use
   */
  def setOptimizer(optimizer: Optimizer): Unit = {
    _optimizer = optimizer
  }

  /**
   * Submit multiple [[PipelineResult]]s to be optimized as a group by looking at all of
   * their underlying execution plans together, instead of optimizing each plan individually.
   * When a workload consists of processing multiple datasets using multiple pipelines,
   * this may produce superior workload throughput.
   *
   * This is a lazy method, i.e. no optimization or execution will occur until the submitted executions have
   * their results accessed.
   *
   * @param executions The executions to submit as a group.
   */
  def submit(executions: PipelineResult[_]*): Unit = {
    // Add all the graphs of each graphBackedExecution into a single merged graph,
    // And all the states of each graphBackedExecution into a single merged state
    val emptyGraph = new Graph(Set(), Map(), Map(), Map())
    val (newGraph, _, sinkMappings) = executions.foldLeft(
      emptyGraph,
      Seq[Map[SourceId, SourceId]](),
      Seq[Map[SinkId, SinkId]]()
    ) {
      case ((curGraph, curSourceMappings, curSinkMappings), graphExecution) =>
        // Merge in an additional execution's graph into the total merged graph
        val (nextGraph, nextSourceMapping, _, nextSinkMapping) =
          curGraph.addGraph(graphExecution.getGraph)

        (nextGraph, curSourceMappings :+ nextSourceMapping, curSinkMappings :+ nextSinkMapping)
    }

    // Create a new executor created that is the merged result of all the individual executors from the above
    // merged graph and merged state, then update every GraphBackedExecution to use the new executor.
    val newExecutor = new GraphExecutor(graph = newGraph)
    for (i <- executions.indices) {
      val execution = executions(i)
      execution.setExecutor(newExecutor)
      execution.setSink(sinkMappings(i).apply(execution.getSink))
    }
  }

  /**
   * Produces a pipeline that when given an input,
   * combines the outputs of all its branches when executed on that input into a single Seq (in order)
   *
   * @param branches The pipelines whose outputs should be combined into a Seq
   */
  def gather[A, B : ClassTag](branches: Seq[Pipeline[A, B]]): Pipeline[A, Seq[B]] = {
    // We initialize to an empty graph with one source
    val source = SourceId(0)
    val emptyGraph = Graph(Set(source), Map(), Map(), Map())

    // We fold the branches together one by one, updating the graph and the overall execution state
    // to include all of the branches.
    val (graphWithAllBranches, branchSinks) = branches.foldLeft(
      emptyGraph,
      Seq[NodeOrSourceId]()) {
      case ((graph, sinks), branch) =>
        // We add the new branch to the graph containing already-processed branches
        val (graphWithBranch, sourceMapping, _, sinkMapping) = graph.addGraph(branch.executor.graph)

        // We then remove the new branch's individual source and make the branch
        // depend on the new joint source for all branches.
        // We also remove the branch's sink.
        val branchSource = sourceMapping(branch.source)
        val branchSink = sinkMapping(branch.sink)
        val branchSinkDep = graphWithBranch.getSinkDependency(branchSink)
        val nextGraph = graphWithBranch.replaceDependency(branchSource, source)
          .removeSource(branchSource)
          .removeSink(branchSink)

        (nextGraph, sinks :+ branchSinkDep)
    }

    // Finally, we add a gather transformer with all of the branches' endpoints as dependencies,
    // and add a new sink on the gather transformer.
    val (graphWithGather, gatherNode) = graphWithAllBranches.addNode(new GatherTransformer[B], branchSinks)
    val (newGraph, sink) = graphWithGather.addSink(gatherNode)

    // We construct & return the new gathered pipeline
    val executor = new GraphExecutor(newGraph)
    new ConcretePipeline[A, Seq[B]](executor, source, sink)
  }
}
