package workflow.graph

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import pipelines.Logging

import scala.reflect.ClassTag

private[graph] class GraphExecutor(graph: Graph, initExecutionState: Map[GraphId, Expression], optimize: Boolean = true) {
  private var optimized: Boolean = false
  private lazy val optimizedState: (Graph, scala.collection.mutable.Map[GraphId, Expression]) = {
    val (newGraph, newExecutionState) = if (optimize) {
      Pipeline.getOptimizer.execute(graph, initExecutionState)
    } else {
      (graph, initExecutionState)
    }

    optimized = true
    (newGraph, scala.collection.mutable.Map() ++ newExecutionState)
  }
  private lazy val optimizedGraph: Graph = optimizedState._1
  private lazy val executionState: scala.collection.mutable.Map[GraphId, Expression] = optimizedState._2

  def currentGraph: Graph = if (optimized) {
    optimizedGraph
  } else {
    graph
  }

  def currentState: Map[GraphId, Expression] = if (optimized) {
    executionState.toMap
  } else {
    initExecutionState
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

abstract class GraphBackedExecution[T](initExecutor: GraphExecutor, initSources: Map[SourceId, Operator], initSink: SinkId, expressionToOut: Expression => T) {
  private var executor: GraphExecutor = initExecutor
  private var sources: Map[SourceId, Operator] = initSources
  private var sink: SinkId = initSink


  protected def getExecutor: GraphExecutor = executor

  private[graph] def setExecutor(executor: GraphExecutor): Unit = {
    this.executor = executor
  }

  private[graph] def setSources(sources: Map[SourceId, Operator]): Unit = {
    this.sources = sources
  }

  private[graph] def setSink(sink: SinkId): Unit = {
    this.sink = sink
  }

  private[graph] def getSources: Map[SourceId, Operator] = sources
  private[graph] def getSink: SinkId = sink

  private var ranExecution: Boolean = false
  private lazy val finalExecutor: GraphExecutor = {
    if (getSources.nonEmpty) {
      getExecutor.executeAndSaveWithoutSources(getSink)

      val unmergedGraph = sources.foldLeft(getExecutor.currentGraph) {
        case (curGraph, (sourceId, sourceOp)) => {
          val (graphWithDataset, nodeId) = getExecutor.currentGraph.addNode(sourceOp, Seq())
          graphWithDataset.replaceDependency(sourceId, nodeId).removeSource(sourceId)
        }
      }

      // Note: The existing executor state should not have any value stored at the source, hence we don't need to update it
      val (newGraph, newState) = EquivalentNodeMergeOptimizer.execute(unmergedGraph, getExecutor.currentState)

      ranExecution = true

      new GraphExecutor(newGraph, newState, optimize = false)
    } else {
      getExecutor
    }
  }

  private[graph] def currentGraph: Graph = if (ranExecution) {
    finalExecutor.currentGraph
  } else {
    sources.foldLeft(getExecutor.currentGraph) {
      case (curGraph, (sourceId, sourceOp)) => {
        val (graphWithDataset, nodeId) = getExecutor.currentGraph.addNode(sourceOp, Seq())
        graphWithDataset.replaceDependency(sourceId, nodeId).removeSource(sourceId)
      }
    }
  }

  private[graph] def currentState: Map[GraphId, Expression] = if (ranExecution) {
    finalExecutor.currentState
  } else {
    getExecutor.currentState
  }

  final def get(): T = expressionToOut(finalExecutor.execute(getSink, Map()))
}

// A lazy representation of a pipeline output
class PipelineDatumOut[T](initExecutor: GraphExecutor, initSink: SinkId, source: Option[(SourceId, Any)])
  extends GraphBackedExecution(
    initExecutor,
    source.map(sourceAndVal => Map(sourceAndVal._1 -> DatumOperator(sourceAndVal._2))).getOrElse(Map()),
    initSink,
    _.asInstanceOf[DatumExpression].get.asInstanceOf[T])

// A lazy representation of a pipeline output
class PipelineDatasetOut[T](initExecutor: GraphExecutor, initSink: SinkId, source: Option[(SourceId, RDD[_])])
  extends GraphBackedExecution(
    initExecutor,
    source.map(sourceAndVal => Map(sourceAndVal._1 -> DatasetOperator(sourceAndVal._2))).getOrElse(Map()),
    initSink,
    _.asInstanceOf[DatasetExpression].get.asInstanceOf[RDD[T]])

object PipelineRDDUtils {
  def rddToPipelineDatasetOut[T](rdd: RDD[T]): PipelineDatasetOut[T] = {
    val emptyGraph = Graph(Set(), Map(), Map(), Map())
    val (graphWithDataset, nodeId) = emptyGraph.addNode(new DatasetOperator(rdd), Seq())
    val (graph, sinkId) = graphWithDataset.addSink(nodeId)

    new PipelineDatasetOut[T](new GraphExecutor(graph, Map()), sinkId, None)
  }

  def datumToPipelineDatumOut[T](datum: T): PipelineDatumOut[T] = {
    val emptyGraph = Graph(Set(), Map(), Map(), Map())
    val (graphWithDataset, nodeId) = emptyGraph.addNode(new DatumOperator(datum), Seq())
    val (graph, sinkId) = graphWithDataset.addSink(nodeId)

    new PipelineDatumOut[T](new GraphExecutor(graph, Map()), sinkId, None)
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
    val (newGraph, newSourceMapping, newNodeMapping, newSinkMapping) = data.currentGraph.connectGraph(executor.currentGraph, Map(source -> data.getSink))
    val graphIdMappings: Map[GraphId, GraphId] = newSourceMapping ++ newNodeMapping ++ newSinkMapping
    val newExecutionState = data.currentState - data.getSink ++ executor.currentState.map(x => (graphIdMappings(x._1), x._2))

    new PipelineDatasetOut[B](new GraphExecutor(newGraph, newExecutionState), newSinkMapping(sink), None)
  }

  final def apply(datum: PipelineDatumOut[A]): PipelineDatumOut[B] = {
    val (newGraph, newSourceMapping, newNodeMapping, newSinkMapping) =
      datum.currentGraph.connectGraph(executor.currentGraph, Map(source -> datum.getSink))
    val graphIdMappings: Map[GraphId, GraphId] = newSourceMapping ++ newNodeMapping ++ newSinkMapping
    val newExecutionState = datum.currentState - datum.getSink ++ executor.currentState.map(x => (graphIdMappings(x._1), x._2))

    new PipelineDatumOut[B](new GraphExecutor(newGraph, newExecutionState), newSinkMapping(sink), None)
  }

  // TODO: Clean up this method
  final def andThen[C](next: Pipeline[B, C]): Pipeline[A, C] = {
    val (newGraph, newSourceMappings, newNodeMappings, newSinkMappings) = executor.currentGraph.connectGraph(next.executor.currentGraph, Map(next.source -> sink))
    val graphIdMappings: Map[GraphId, GraphId] = newSourceMappings ++ newNodeMappings ++ newSinkMappings
    val newExecutionState = executor.currentState - sink ++ next.executor.currentState.map(x => (graphIdMappings(x._1), x._2))
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

class ConcretePipeline[A, B](
  @Override private[graph] val executor: GraphExecutor,
  @Override private[graph] val source: SourceId,
  @Override private[graph] val sink: SinkId
) extends Pipeline[A, B]

abstract class Transformer[A, B : ClassTag] extends TransformerOperator with Pipeline[A, B] {
  @Override @transient private[graph] lazy val executor = new GraphExecutor(Graph(Set(SourceId(0)), Map(SinkId(0) -> NodeId(0)), Map(NodeId(0) -> this), Map(NodeId(0) -> Seq(SourceId(0)))), Map())
  @Override private[graph] val source = SourceId(0)
  @Override private[graph] val sink = SinkId(0)

  protected def singleTransform(in: A): B
  protected def batchTransform(in: RDD[A]): RDD[B] = in.map(singleTransform)

  final override private[graph] def singleTransform(inputs: Seq[DatumExpression]): Any = {
    singleTransform(inputs.head.get.asInstanceOf[A])
  }

  final override private[graph] def batchTransform(inputs: Seq[DatasetExpression]): RDD[_] = {
    batchTransform(inputs.head.get.asInstanceOf[RDD[A]])
  }
}

object Transformer {
  /**
   * This constructor takes a function and returns a Transformer that maps it over the input RDD
   *
   * @param f The function to apply to every item in the RDD being transformed
   * @tparam I input type of the transformer
   * @tparam O output type of the transformer
   * @return Transformer that applies the given function to all items in the RDD
   */
  def apply[I, O : ClassTag](f: I => O): Transformer[I, O] = new Transformer[I, O] {
    override protected def batchTransform(in: RDD[I]): RDD[O] = in.map(f)
    override protected def singleTransform(in: I): O = f(in)
  }
}


abstract class Estimator[A, B] extends EstimatorOperator {
  final def fit(data: RDD[A]): Pipeline[A, B] = fit(PipelineRDDUtils.rddToPipelineDatasetOut(data))
  final def fit(data: PipelineDatasetOut[A]): Pipeline[A, B] = {
    val curSink = data.currentGraph.getSinkDependency(data.getSink)
    val (newGraph, nodeId) = data.currentGraph.removeSink(data.getSink).addNode(this, Seq(curSink))
    val (newGraphWithSource, sourceId) = newGraph.addSource()
    val (almostFinalGraph, delegatingId) = newGraphWithSource.addNode(new DelegatingOperator, Seq(nodeId, sourceId))
    val (finalGraph, sinkId) = almostFinalGraph.addSink(delegatingId)

    new ConcretePipeline(new GraphExecutor(finalGraph, data.currentState - data.getSink), sourceId, sinkId)
  }

  final override private[graph] def fitRDDs(inputs: Seq[DatasetExpression]): TransformerOperator = {
    fitRDD(inputs.head.get.asInstanceOf[RDD[A]])
  }
  protected def fitRDD(data: RDD[A]): Transformer[A, B]
}

abstract class LabelEstimator[A, B, L] extends EstimatorOperator {
  final def fit(data: RDD[A], labels: PipelineDatasetOut[L]): Pipeline[A, B] = fit(PipelineRDDUtils.rddToPipelineDatasetOut(data), labels)
  final def fit(data: PipelineDatasetOut[A], labels: RDD[L]): Pipeline[A, B] = fit(data, PipelineRDDUtils.rddToPipelineDatasetOut(labels))
  final def fit(data: RDD[A], labels: RDD[L]): Pipeline[A, B] = fit(PipelineRDDUtils.rddToPipelineDatasetOut(data), PipelineRDDUtils.rddToPipelineDatasetOut(labels))
  final def fit(data: PipelineDatasetOut[A], labels: PipelineDatasetOut[L]): Pipeline[A, B] = {
    val (depGraph, labelSourceMapping, labelNodeMapping, labelSinkMapping) = data.currentGraph.addGraph(labels.currentGraph)
    val dataSink = depGraph.getSinkDependency(data.getSink)
    val labelsSink = depGraph.getSinkDependency(labelSinkMapping(labels.getSink))
    val (newGraph, nodeId) = depGraph
      .removeSink(data.getSink)
      .removeSink(labelSinkMapping(labels.getSink))
      .addNode(this, Seq(dataSink, labelsSink))
    val (newGraphWithSource, sourceId) = newGraph.addSource()
    val (almostFinalGraph, delegatingId) = newGraphWithSource.addNode(new DelegatingOperator, Seq(nodeId, sourceId))
    val (finalGraph, sinkId) = almostFinalGraph.addSink(delegatingId)

    val graphIdMappings: Map[GraphId, GraphId] = labelSourceMapping ++ labelNodeMapping ++ labelSinkMapping
    val newExecutionState = data.currentState ++ labels.currentState.map(x => (graphIdMappings(x._1), x._2)) - data.getSink - labelSinkMapping(labels.getSink)

    new ConcretePipeline(new GraphExecutor(finalGraph, newExecutionState), sourceId, sinkId)
  }

  final override private[graph] def fitRDDs(inputs: Seq[DatasetExpression]): TransformerOperator = {
    fitRDDs(inputs(0).get.asInstanceOf[RDD[A]], inputs(1).get.asInstanceOf[RDD[L]])
  }
  protected def fitRDDs(data: RDD[A], labels: RDD[L]): Transformer[A, B]

}

case class Identity[T : ClassTag]() extends Transformer[T,T] {
  override protected def singleTransform(in: T): T = in
  override protected def batchTransform(in: RDD[T]): RDD[T] = in
}

/**
 * Caches the intermediate state of a node. Follows Spark's lazy evaluation conventions.
 *
 * @param name An optional name to set on the cached output. Useful for debugging.
 * @tparam T Type of the input to cache.
 */
case class Cacher[T: ClassTag](name: Option[String] = None) extends Transformer[T,T] with Logging {
  override protected def batchTransform(in: RDD[T]): RDD[T] = {
    logInfo(s"CACHING ${in.id}")
    name match {
      case Some(x) => in.cache().setName(x)
      case None => in.cache()
    }
  }

  override protected def singleTransform(in: T): T = in
}

case class Checkpointer[T : ClassTag](path: String) extends Estimator[T, T] {
  override protected def fitRDD(data: RDD[T]): Transformer[T, T] = {
    data.saveAsObjectFile(path)
    new Identity[T]
  }
}

object Pipeline {
  private var globalOptimizer: Optimizer = DefaultOptimizer
  def getOptimizer: Optimizer = globalOptimizer
  def setOptimizer(optimizer: Optimizer): Unit = {
    globalOptimizer = optimizer
  }

  // If the checkpoint is found, return an output that just reads it from disk.
  // If the checkpoint is not found, return the input data graph w/ an EstimatorOperator just saves to disk added at the end
  def checkpoint[T : ClassTag](data: PipelineDatasetOut[T], path: String, sc: SparkContext, minPartitions: Int): PipelineDatasetOut[T] = {
    val filePath = new Path(path)
    val conf = new Configuration(true)
    val fs = FileSystem.get(filePath.toUri(), conf)
    if (fs.exists(filePath)) {
      PipelineRDDUtils.rddToPipelineDatasetOut(sc.objectFile(path, minPartitions))
    } else {
      Checkpointer[T](path).fit(data).apply(data)
    }
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
        val (nextGraph, nextSourceMapping, nextNodeMapping, nextSinkMapping) = curGraph.addGraph(graphExecution.currentGraph)
        val graphIdMappings: Map[GraphId, GraphId] = nextSourceMapping ++ nextNodeMapping ++ nextSinkMapping
        val nextExecutionState = curExecutionState ++ graphExecution.currentState.map(x => (graphIdMappings(x._1), x._2))

        (nextGraph, nextExecutionState, curSourceMappings :+ nextSourceMapping, curSinkMappings :+ nextSinkMapping)
    }

    val newExecutor = new GraphExecutor(graph = newGraph, initExecutionState = newExecutionState)
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
   * @param branches The pipelines whose outputs should be combined into a Seq
   */
  def gather[A, B : ClassTag](branches: Seq[Pipeline[A, B]]): Pipeline[A, Seq[B]] = {
    val source = SourceId(0)
    val emptyGraph = Graph(Set(source), Map(), Map(), Map())
    val (graphWithAllBranches, newState, branchSinks) = branches.foldLeft(
      emptyGraph,
      Map[GraphId, Expression](),
      Seq[NodeOrSourceId]()) {
      case ((graph, state, sinks), branch) =>
        val (graphWithBranch, sourceMapping, nodeMapping, sinkMapping) = graph.addGraph(branch.executor.currentGraph)
        val branchSource = sourceMapping(branch.source)
        val branchSink = sinkMapping(branch.sink)
        val branchSinkDep = graphWithBranch.getSinkDependency(branchSink)
        val nextGraph = graphWithBranch.replaceDependency(branchSource, source).removeSource(branchSource).removeSink(branchSink)
        val graphIdMappings: Map[GraphId, GraphId] = sourceMapping ++ nodeMapping ++ sinkMapping
        val nextState = state ++ branch.executor.currentState.map(x => (graphIdMappings(x._1), x._2)) - branchSink - branchSource

        (nextGraph, nextState, sinks :+ branchSinkDep)
    }

    val (graphWithGather, gatherNode) = graphWithAllBranches.addNode(new GatherTransformer[B], branchSinks)
    val (newGraph, sink) = graphWithGather.addSink(gatherNode)
    val executor = new GraphExecutor(newGraph, newState)
    new ConcretePipeline[A, Seq[B]](executor, source, sink)
  }

}