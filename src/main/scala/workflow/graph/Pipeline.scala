package workflow.graph

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

private[graph] class GraphExecutor(graph: Graph, state: Map[GraphId, Expression], optimize: Boolean = true) {
  private var optimized: Boolean = false
  private lazy val optimizedState: (Graph, scala.collection.mutable.Map[GraphId, Expression]) = {
    val (newGraph, newExecutionState) = if (optimize) {
      Pipeline.getOptimizer.execute(graph, state)
    } else {
      (graph, state)
    }

    optimized = true
    (newGraph, scala.collection.mutable.Map() ++ newExecutionState)
  }
  private lazy val optimizedGraph: Graph = optimizedState._1
  private lazy val executionState: scala.collection.mutable.Map[GraphId, Expression] = optimizedState._2

  def getGraph: Graph = if (optimized) {
    optimizedGraph
  } else {
    graph
  }

  def getState: Map[GraphId, Expression] = if (optimized) {
    executionState.toMap
  } else {
    state
  }

  // Todo put comment: A result is unstorable if it implicitly depends on any source
  private lazy val unstorableResults: Set[GraphId] = {
    optimizedGraph.sources.foldLeft(Set[GraphId]()) {
      case (descendants, source) => descendants ++ AnalysisUtils.getDescendants(optimizedGraph, source) + source
    }
  }

  // TODO rename and comment. This method executes & stores all ancestors of a sink that don't depend on sources, and haven't already been executed
  def executeAndSaveWithoutSources(sink: SinkId): Unit = {
    val linearizedAncestors = AnalysisUtils.linearize(optimizedGraph, sink)
    val ancestorsToExecute = linearizedAncestors.filter(id => (!unstorableResults.contains(id)) && (!executionState.contains(id)))
    ancestorsToExecute.foreach {
      case source: SourceId => throw new RuntimeException("Linearized ancestors to execute should not contain sources")
      case node: NodeId => {
        val dependencies = optimizedGraph.getDependencies(node)
        val depExpressions = dependencies.map(dep => executionState(dep))
        val operator = optimizedGraph.getOperator(node)
        executionState(node) = operator.execute(depExpressions)
      }
    }

    val sinkDep = optimizedGraph.getSinkDependency(sink)
    if (ancestorsToExecute.contains(sinkDep) && (!executionState.contains(sink))) {
      executionState(sink) = executionState(sinkDep)
    }
  }

  private def getUncachedResult(graphId: GraphId, sources: Map[SourceId, Expression]): Expression = {
    graphId match {
      case source: SourceId => sources.get(source).get
      case node: NodeId => {
        val dependencies = optimizedGraph.getDependencies(node)
        val depExpressions = dependencies.map(dep => getResult(dep, sources))
        val operator = optimizedGraph.getOperator(node)
        operator.execute(depExpressions)
      }
      case sink: SinkId => {
        val sinkDep = optimizedGraph.getSinkDependency(sink)
        getResult(sinkDep, sources)
      }
    }
  }

  private def getResult(graphId: GraphId, sources: Map[SourceId, Expression]): Expression = {
    if (unstorableResults.contains(graphId)) {
      getUncachedResult(graphId, sources)
    } else {
      executionState.getOrElseUpdate(graphId, getUncachedResult(graphId, sources))
    }
  }

  def execute(sinkId: SinkId, sources: Map[SourceId, Expression]): Expression = {
    getResult(sinkId, sources)
  }
}

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
      getExecutor.executeAndSaveWithoutSources(getSink)

      val unmergedGraph = _sources.foldLeft(getExecutor.getGraph) {
        case (curGraph, (sourceId, sourceOp)) => {
          val (graphWithDataset, nodeId) = getExecutor.getGraph.addNode(sourceOp, Seq())
          graphWithDataset.replaceDependency(sourceId, nodeId).removeSource(sourceId)
        }
      }

      // Note: The existing executor state should not have any value stored at the source, hence we don't need to update it
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

  final def get(): T = expressionToOutput(finalExecutor.execute(getSink, Map()))
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
    val (newGraph, newSourceMapping, newNodeMapping, newSinkMapping) = data.getGraph.connectGraph(executor.getGraph, Map(source -> data.getSink))
    val graphIdMappings: Map[GraphId, GraphId] = newSourceMapping ++ newNodeMapping ++ newSinkMapping
    val newExecutionState = data.getState - data.getSink ++ executor.getState.map(x => (graphIdMappings(x._1), x._2))

    new PipelineDatasetOut[B](new GraphExecutor(newGraph, newExecutionState), newSinkMapping(sink), None)
  }

  final def apply(datum: PipelineDatumOut[A]): PipelineDatumOut[B] = {
    val (newGraph, newSourceMapping, newNodeMapping, newSinkMapping) =
      datum.getGraph.connectGraph(executor.getGraph, Map(source -> datum.getSink))
    val graphIdMappings: Map[GraphId, GraphId] = newSourceMapping ++ newNodeMapping ++ newSinkMapping
    val newExecutionState = datum.getState - datum.getSink ++ executor.getState.map(x => (graphIdMappings(x._1), x._2))

    new PipelineDatumOut[B](new GraphExecutor(newGraph, newExecutionState), newSinkMapping(sink), None)
  }

  // TODO: Clean up this method
  final def andThen[C](next: Pipeline[B, C]): Pipeline[A, C] = {
    val (newGraph, newSourceMappings, newNodeMappings, newSinkMappings) = executor.getGraph.connectGraph(next.executor.getGraph, Map(next.source -> sink))
    val graphIdMappings: Map[GraphId, GraphId] = newSourceMappings ++ newNodeMappings ++ newSinkMappings
    val newExecutionState = executor.getState - sink ++ next.executor.getState.map(x => (graphIdMappings(x._1), x._2))
    new ConcretePipeline(new GraphExecutor(newGraph, newExecutionState), source, newSinkMappings(next.sink))
  }

  final def andThen[C](est: Estimator[B, C], data: RDD[A]): Pipeline[A, C] = {
    this andThen est.fit(apply(data))
  }

  final def andThen[C](est: Estimator[B, C], data: PipelineDatasetOut[A]): Pipeline[A, C] = {
    this andThen est.fit(apply(data))
  }

  final def andThen[C, L](est: LabelEstimator[B, C, L], data: RDD[A], labels: RDD[L]): Pipeline[A, C] = {
    this andThen est.fit(apply(data), labels)
  }

  final def andThen[C, L](est: LabelEstimator[B, C, L], data: PipelineDatasetOut[A], labels: RDD[L]): Pipeline[A, C] = {
    this andThen est.fit(apply(data), labels)
  }

  final def andThen[C, L](est: LabelEstimator[B, C, L], data: RDD[A], labels: PipelineDatasetOut[L]): Pipeline[A, C] = {
    this andThen est.fit(apply(data), labels)
  }

  final def andThen[C, L](est: LabelEstimator[B, C, L], data: PipelineDatasetOut[A], labels: PipelineDatasetOut[L]): Pipeline[A, C] = {
    this andThen est.fit(apply(data), labels)
  }

}

private[graph] class ConcretePipeline[A, B](
  override private[graph] val executor: GraphExecutor,
  override private[graph] val source: SourceId,
  override private[graph] val sink: SinkId
) extends Pipeline[A, B]

object Pipeline {
  private var _optimizer: Optimizer = DefaultOptimizer
  def getOptimizer: Optimizer = _optimizer
  def setOptimizer(optimizer: Optimizer): Unit = {
    _optimizer = optimizer
  }

  // Combine all the internal graph representations to use the same, merged representation
  def submit(graphBackedExecutions: GraphBackedExecution[_]*): Unit = {
    val emptyGraph = new Graph(Set(), Map(), Map(), Map())
    val (newGraph, newExecutionState, _, sinkMappings) = graphBackedExecutions.foldLeft(
      emptyGraph,
      Map[GraphId, Expression](),
      Seq[Map[SourceId, SourceId]](),
      Seq[Map[SinkId, SinkId]]()
    ) {
      case ((curGraph, curExecutionState, curSourceMappings, curSinkMappings), graphExecution) =>
        val (nextGraph, nextSourceMapping, nextNodeMapping, nextSinkMapping) = curGraph.addGraph(graphExecution.getGraph)
        val graphIdMappings: Map[GraphId, GraphId] = nextSourceMapping ++ nextNodeMapping ++ nextSinkMapping
        val nextExecutionState = curExecutionState ++ graphExecution.getState.map(x => (graphIdMappings(x._1), x._2))

        (nextGraph, nextExecutionState, curSourceMappings :+ nextSourceMapping, curSinkMappings :+ nextSinkMapping)
    }

    val newExecutor = new GraphExecutor(graph = newGraph, state = newExecutionState)
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
        val graphIdMappings: Map[GraphId, GraphId] = sourceMapping ++ nodeMapping ++ sinkMapping
        val branchState = branch.executor.getState.map(x => (graphIdMappings(x._1), x._2)) - branchSink - branchSource
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
