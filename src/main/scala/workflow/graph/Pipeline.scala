package workflow.graph

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * A Pipeline takes data as input (single item or an RDD), and outputs some transformation
 * of that data. Internally, a Pipeline contains a [[GraphExecutor]], a specified source, and a specified sink.
 * When a pipeline is applied to data it produces a [[GraphExecution]], in the form of either a [[PipelineDatasetOut]]
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
   * @return A lazy wrapper around the result of passing the datum through the pipeline.
   */
  final def apply(datum: A): PipelineDatumOut[B] = {
    new PipelineDatumOut[B](executor, sink, Some(source, datum))
  }

  /**
   * Lazily apply the pipeline to a dataset.
   * @return A lazy wrapper around the result of passing the dataset through the pipeline.
   */
  final def apply(data: RDD[A]): PipelineDatasetOut[B] = {
    new PipelineDatasetOut[B](executor, sink, Some(source, data))
  }

  /**
   * Lazily apply the pipeline to the lazy output of a different pipeline given an initial dataset.
   * If the previous pipeline has already been fit, it will not need to be fit again.
   * @return A lazy wrapper around the result of passing lazy output from a different pipeline through this pipeline.
   */
  final def apply(data: PipelineDatasetOut[A]): PipelineDatasetOut[B] = {
    val (newGraph, sourceMapping, nodeMapping, sinkMapping) =
      data.getGraph.connectGraph(executor.getGraph, Map(source -> data.getSink))
    val idMapping: Map[GraphId, GraphId] = sourceMapping ++ nodeMapping ++ sinkMapping
    val newState = data.getState - data.getSink ++ executor.getState.map(x => (idMapping(x._1), x._2))

    new PipelineDatasetOut[B](new GraphExecutor(newGraph, newState), sinkMapping(sink), None)
  }

  /**
   * Lazily apply the pipeline to the lazy output of a different pipeline given an initial datum.
   * If the previous pipeline has already been fit, it will not need to be fit again.
   * @return A lazy wrapper around the result of passing lazy output from a different pipeline through this pipeline.
   */
  final def apply(datum: PipelineDatumOut[A]): PipelineDatumOut[B] = {
    val (newGraph, sourceMapping, nodeMapping, sinkMapping) =
      datum.getGraph.connectGraph(executor.getGraph, Map(source -> datum.getSink))
    val idMapping: Map[GraphId, GraphId] = sourceMapping ++ nodeMapping ++ sinkMapping
    val newState = datum.getState - datum.getSink ++ executor.getState.map(x => (idMapping(x._1), x._2))

    new PipelineDatumOut[B](new GraphExecutor(newGraph, newState), sinkMapping(sink), None)
  }

  /**
   * Chains a pipeline onto the end of this one, producing a new pipeline.
   * If either this pipeline or the following has already been fit, it will not need to be fit again.
   *
   * @param next the pipeline to chain
   */
  final def andThen[C](next: Pipeline[B, C]): Pipeline[A, C] = {
    val (newGraph, sourceMapping, nodeMapping, sinkMapping) =
      executor.getGraph.connectGraph(next.executor.getGraph, Map(next.source -> sink))
    val idMapping: Map[GraphId, GraphId] = sourceMapping ++ nodeMapping ++ sinkMapping
    val newState = executor.getState - sink ++ next.executor.getState.map(x => (idMapping(x._1), x._2))
    new ConcretePipeline(new GraphExecutor(newGraph, newState), source, sinkMapping(next.sink))
  }

  final def andThen[C](est: Estimator[B, C], data: RDD[A]): Pipeline[A, C] = {
    this andThen est.fit(apply(data))
  }

  final def andThen[C](est: Estimator[B, C], data: PipelineDatasetOut[A]): Pipeline[A, C] = {
    this andThen est.fit(apply(data))
  }

  final def andThen[C, L](
    est: LabelEstimator[B, C, L],
    data: RDD[A],
    labels: RDD[L]
  ): Pipeline[A, C] = {
    this andThen est.fit(apply(data), labels)
  }

  final def andThen[C, L](
    est: LabelEstimator[B, C, L],
    data: PipelineDatasetOut[A],
    labels: RDD[L]
  ): Pipeline[A, C] = {
    this andThen est.fit(apply(data), labels)
  }

  final def andThen[C, L](
    est: LabelEstimator[B, C, L],
    data: RDD[A],
    labels: PipelineDatasetOut[L]
  ): Pipeline[A, C] = {
    this andThen est.fit(apply(data), labels)
  }

  final def andThen[C, L](
    est: LabelEstimator[B, C, L],
    data: PipelineDatasetOut[A],
    labels: PipelineDatasetOut[L]
  ): Pipeline[A, C] = {
    this andThen est.fit(apply(data), labels)
  }

}

object Pipeline {
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
   * Submit multiple [[GraphExecution]]s to be optimized as a group by looking at all of
   * their underlying execution plans together, instead of optimizing each plan individually.
   * When a workload consists of processing multiple datasets using multiple pipelines,
   * this may produce superior workload throughput.
   *
   * This is a lazy method, i.e. no optimization or execution will occur until the submitted executions have
   * their results accessed.
   *
   * @param executions The executions to submit as a group.
   */
  def submit(executions: GraphExecution[_]*): Unit = {
    // Add all the graphs of each graphBackedExecution into a single merged graph,
    // And all the states of each graphBackedExecution into a single merged state
    val emptyGraph = new Graph(Set(), Map(), Map(), Map())
    val (newGraph, newState, _, sinkMappings) = executions.foldLeft(
      emptyGraph,
      Map[GraphId, Expression](),
      Seq[Map[SourceId, SourceId]](),
      Seq[Map[SinkId, SinkId]]()
    ) {
      case ((curGraph, curState, curSourceMappings, curSinkMappings), graphExecution) =>
        // Merge in an additional execution's graph into the total merged graph
        val (nextGraph, nextSourceMapping, nextNodeMapping, nextSinkMapping) =
          curGraph.addGraph(graphExecution.getGraph)

        // Merge in an additional execution's state into the total merged state
        val idMapping: Map[GraphId, GraphId] = nextSourceMapping ++ nextNodeMapping ++ nextSinkMapping
        val nextState = curState ++ graphExecution.getState.map(x => (idMapping(x._1), x._2))

        (nextGraph, nextState, curSourceMappings :+ nextSourceMapping, curSinkMappings :+ nextSinkMapping)
    }

    // Create a new executor created that is the merged result of all the individual executors from the above
    // merged graph and merged state, then update every GraphBackedExecution to use the new executor.
    val newExecutor = new GraphExecutor(graph = newGraph, state = newState)
    for (i <- executions.indices) {
      val execution = executions(i)
      execution.setExecutor(newExecutor)
      execution.setSources(Map())
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
    val (graphWithAllBranches, newState, branchSinks) = branches.foldLeft(
      emptyGraph,
      Map[GraphId, Expression](),
      Seq[NodeOrSourceId]()) {
      case ((graph, state, sinks), branch) =>
        // We add the new branch to the graph containing already-processed branches
        val (graphWithBranch, sourceMapping, nodeMapping, sinkMapping) = graph.addGraph(branch.executor.getGraph)

        // We then remove the new branch's individual source and make the branch
        // depend on the new joint source for all branches.
        // We also remove the branch's sink.
        val branchSource = sourceMapping(branch.source)
        val branchSink = sinkMapping(branch.sink)
        val branchSinkDep = graphWithBranch.getSinkDependency(branchSink)
        val nextGraph = graphWithBranch.replaceDependency(branchSource, source)
          .removeSource(branchSource)
          .removeSink(branchSink)

        // Because pipeline construction is incremental, we make sure to add the state of the branch to our
        // accumulated state. We update the graph ids for the new branch's state, and remove all graph ids
        // that no longer exist.
        val idMapping: Map[GraphId, GraphId] = sourceMapping ++ nodeMapping ++ sinkMapping
        val branchState = branch.executor.getState.map(x => (idMapping(x._1), x._2)) - branchSink - branchSource
        val nextState = state ++ branchState

        (nextGraph, nextState, sinks :+ branchSinkDep)
    }

    // Finally, we add a gather transformer with all of the branches' endpoints as dependencies,
    // and add a new sink on the gather transformer.
    val (graphWithGather, gatherNode) = graphWithAllBranches.addNode(new GatherTransformer[B], branchSinks)
    val (newGraph, sink) = graphWithGather.addSink(gatherNode)

    // We construct & return the new gathered pipeline
    val executor = new GraphExecutor(newGraph, newState)
    new ConcretePipeline[A, Seq[B]](executor, source, sink)
  }
}
