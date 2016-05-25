package workflow.graph

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

private[graph] class GraphExecutor(graph: Graph, state: Map[GraphId, Expression], optimize: Boolean = true) {
  private var optimized: Boolean = false
  private lazy val optimizedGraphAndState: (Graph, scala.collection.mutable.Map[GraphId, Expression]) = {
    val (newGraph, newState) = if (optimize) {
      Pipeline.getOptimizer.execute(graph, state)
    } else {
      (graph, state)
    }

    optimized = true
    (newGraph, scala.collection.mutable.Map() ++ newState)
  }
  private lazy val optimizedGraph: Graph = optimizedGraphAndState._1
  private lazy val optimizedState: scala.collection.mutable.Map[GraphId, Expression] = optimizedGraphAndState._2

  def getGraph: Graph = if (optimized) {
    optimizedGraph
  } else {
    graph
  }

  def getState: Map[GraphId, Expression] = if (optimized) {
    optimizedState.toMap
  } else {
    state
  }

  private lazy val sourceDependants: Set[GraphId] = {
    optimizedGraph.sources.foldLeft(Set[GraphId]()) {
      case (descendants, source) => descendants ++ AnalysisUtils.getDescendants(optimizedGraph, source) + source
    }
  }

  def partialExecute(graphId: GraphId): Unit = {
    val linearization = AnalysisUtils.linearize(optimizedGraph, graphId) :+ graphId
    val idsToExecute = linearization.filter(id => (!sourceDependants.contains(id)) && (!optimizedState.contains(id)))
    idsToExecute.foreach {ancestor => execute(ancestor)}
  }

  def execute(graphId: GraphId): Expression = {
    require(!sourceDependants.contains(graphId), "May not execute GraphIds that depend on unconnected sources.")

    optimizedState.getOrElseUpdate(graphId, {
      graphId match {
        case source: SourceId => throw new IllegalArgumentException("Unconnected sources have no stored value")
        case node: NodeId => {
          val dependencies = optimizedGraph.getDependencies(node)
          val depExpressions = dependencies.map(dep => execute(dep))
          val operator = optimizedGraph.getOperator(node)
          operator.execute(depExpressions)
        }
        case sink: SinkId => {
          val sinkDep = optimizedGraph.getSinkDependency(sink)
          execute(sinkDep)
        }
      }
    })
  }
}

// rough idea given incrementalism: do everything it can in the base executor (which may be shared w/ other things) w/o inserting sources.
// then create a "final executor" that is the base one w/ sources inserted, and optimized using the EquivalentNodeMerge optimizer.
// The final value execution happens on that "final executor"
// This two stage process allows "intuitive things" to happen a source being passed in is already processed elsewhere in the pipeline (e.g. making sure to reuse a cacher),
// while pipeline fitting results can be reused across multiple pipeline applies, as they all share the same base executor.
abstract class GraphBackedExecution[T](
  executor: GraphExecutor,
  sources: Map[SourceId, Operator],
  sink: SinkId,
  expressionToOutput: Expression => T
) {
  private var _executor: GraphExecutor = executor
  private var _sources: Map[SourceId, Operator] = sources
  private var _sink: SinkId = sink

  protected def getExecutor: GraphExecutor = _executor

  private[graph] def setExecutor(executor: GraphExecutor): Unit = {
    this._executor = executor
  }

  private[graph] def setSources(sources: Map[SourceId, Operator]): Unit = {
    this._sources = sources
  }

  private[graph] def setSink(sink: SinkId): Unit = {
    this._sink = sink
  }

  private[graph] def getSources: Map[SourceId, Operator] = _sources
  private[graph] def getSink: SinkId = _sink

  private var ranExecution: Boolean = false
  private lazy val finalExecutor: GraphExecutor = {
    if (getSources.nonEmpty) {
      getExecutor.partialExecute(getSink)

      val unmergedGraph = _sources.foldLeft(getExecutor.getGraph) {
        case (curGraph, (sourceId, sourceOp)) => {
          val (graphWithDataset, nodeId) = getExecutor.getGraph.addNode(sourceOp, Seq())
          graphWithDataset.replaceDependency(sourceId, nodeId).removeSource(sourceId)
        }
      }

      // Note: The existing executor state should not have any value stored at the removed source,
      // hence we can just reuse it
      val (newGraph, newState) = EquivalentNodeMergeOptimizer.execute(unmergedGraph, getExecutor.getState)

      ranExecution = true

      new GraphExecutor(newGraph, newState, optimize = false)
    } else {
      getExecutor
    }
  }

  private[graph] def getGraph: Graph = if (ranExecution) {
    finalExecutor.getGraph
  } else {
    _sources.foldLeft(getExecutor.getGraph) {
      case (curGraph, (sourceId, sourceOp)) => {
        val (graphWithDataset, nodeId) = getExecutor.getGraph.addNode(sourceOp, Seq())
        graphWithDataset.replaceDependency(sourceId, nodeId).removeSource(sourceId)
      }
    }
  }

  private[graph] def getState: Map[GraphId, Expression] = if (ranExecution) {
    finalExecutor.getState
  } else {
    getExecutor.getState
  }

  final def get(): T = expressionToOutput(finalExecutor.execute(getSink))
}

// A lazy representation of a pipeline output
class PipelineDatumOut[T] private[graph] (executor: GraphExecutor, sink: SinkId, source: Option[(SourceId, Any)])
  extends GraphBackedExecution(
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

// A lazy representation of a pipeline output
class PipelineDatasetOut[T] private[graph] (executor: GraphExecutor, sink: SinkId, source: Option[(SourceId, RDD[_])])
  extends GraphBackedExecution(
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

trait Pipeline[A, B] {
  private[graph] val source: SourceId
  private[graph] val sink: SinkId
  private[graph] def executor: GraphExecutor

  final def apply(datum: A): PipelineDatumOut[B] = {
    new PipelineDatumOut[B](executor, sink, Some(source, datum))
  }

  final def apply(data: RDD[A]): PipelineDatasetOut[B] = {
    new PipelineDatasetOut[B](executor, sink, Some(source, data))
  }

  final def apply(data: PipelineDatasetOut[A]): PipelineDatasetOut[B] = {
    val (newGraph, sourceMapping, nodeMapping, sinkMapping) =
      data.getGraph.connectGraph(executor.getGraph, Map(source -> data.getSink))
    val idMapping: Map[GraphId, GraphId] = sourceMapping ++ nodeMapping ++ sinkMapping
    val newState = data.getState - data.getSink ++ executor.getState.map(x => (idMapping(x._1), x._2))

    new PipelineDatasetOut[B](new GraphExecutor(newGraph, newState), sinkMapping(sink), None)
  }

  final def apply(datum: PipelineDatumOut[A]): PipelineDatumOut[B] = {
    val (newGraph, sourceMapping, nodeMapping, sinkMapping) =
      datum.getGraph.connectGraph(executor.getGraph, Map(source -> datum.getSink))
    val idMapping: Map[GraphId, GraphId] = sourceMapping ++ nodeMapping ++ sinkMapping
    val newState = datum.getState - datum.getSink ++ executor.getState.map(x => (idMapping(x._1), x._2))

    new PipelineDatumOut[B](new GraphExecutor(newGraph, newState), sinkMapping(sink), None)
  }

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
   * @param optimizer The new optimizer to use
   */
  def setOptimizer(optimizer: Optimizer): Unit = {
    _optimizer = optimizer
  }

  /**
   * Submit multiple [[GraphBackedExecution]]s to be optimized as a group by looking at all of
   * their underlying execution plans together, instead of optimizing each plan individually.
   * When a workload consists of processing multiple datasets using multiple pipelines,
   * this may produce superior workload throughput.
   *
   * This is a lazy method, i.e. no optimization or execution will occur until the submitted executions have
   * their results accessed.
   *
   * @param graphBackedExecutions The executions to submit as a group.
   */
  def submit(graphBackedExecutions: GraphBackedExecution[_]*): Unit = {
    // Add all the graphs of each graphBackedExecution into a single merged graph,
    // And all the states of each graphBackedExecution into a single merged state
    val emptyGraph = new Graph(Set(), Map(), Map(), Map())
    val (newGraph, newState, _, sinkMappings) = graphBackedExecutions.foldLeft(
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
    for (i <- graphBackedExecutions.indices) {
      val execution = graphBackedExecutions(i)
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
