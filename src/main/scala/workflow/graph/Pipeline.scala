package workflow.graph

import org.apache.spark.rdd.RDD

class GraphExecutor(val graph: Graph, val optimizer: Option[Optimizer]) {
  private lazy val optimizedGraph: Graph = optimizer.map(_.execute(graph)).getOrElse(graph)

  // Todo put comment: A result is unstorable if it implicitly depends on any source
  private lazy val unstorableResults: Set[GraphId] = {
    optimizedGraph.sources.foldLeft(Set[GraphId]()) {
      case (descendants, source) => descendants ++ AnalysisUtils.getDescendants(optimizedGraph, source) + source
    }
  }

  private val executionResults: scala.collection.mutable.Map[GraphId, Expression] = scala.collection.mutable.Map()

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
      executionResults.getOrElseUpdate(graphId, getUncachedResult(graphId, sources))
    }
  }

  def execute(sinkId: SinkId, sources: Map[SourceId, Expression]): Expression = {
    getResult(sinkId, sources)
  }
}

trait GraphBackedExecution {
  protected var executor: GraphExecutor
  protected var sources: Seq[SourceId]
  protected var sinks: Seq[SinkId]
}

// A lazy representation of a pipeline output
class PipelineDatumOut[T](protected var executor: GraphExecutor, sink: SinkId) extends GraphBackedExecution {
  protected var sources = Seq()
  protected var sinks = Seq(sink)

  def get(): T = executor.execute(sinks.head, Map()).asInstanceOf[DatumExpression].get.asInstanceOf[T]
}

// A lazy representation of a pipeline output
class PipelineDatasetOut[T](protected var executor: GraphExecutor, sink: SinkId) extends GraphBackedExecution {
  protected var sources = Seq()
  protected var sinks = Seq(sink)

  def get(): RDD[T] = executor.execute(sinks.head, Map()).asInstanceOf[DatasetExpression].get.asInstanceOf[RDD[T]]
}

class Pipeline[A, B](protected var executor: GraphExecutor, source: SourceId, sink: SinkId) {
  // These three vars point to the internal representation of this pipeline
  protected var sources = Seq(source)
  protected var sinks = Seq(sink)

  def apply(datum: A): PipelineDatumOut[B] = {
    val (graphWithDatum, datumId) = executor.graph.addNode(new DatumOperator(datum), Seq())
    val newGraph = graphWithDatum.replaceDependency(sources.head, datumId)
      .removeSource(sources.head)

    new PipelineDatumOut[B](new GraphExecutor(newGraph, executor.optimizer), sinks.head)
  }

  def apply(data: RDD[A]): PipelineDatasetOut[B] = {
    val (graphWithDatum, datumId) = executor.graph.addNode(new DatasetOperator(data), Seq())
    val newGraph = graphWithDatum.replaceDependency(sources.head, datumId)
      .removeSource(sources.head)

    new PipelineDatasetOut[B](new GraphExecutor(newGraph, executor.optimizer), sinks.head)
  }

  def apply(data: PipelineDatasetOut[A]): PipelineDatasetOut[B]
}

abstract class Transformer[A, B] extends Pipeline[A, B]


abstract class Estimator[A, B] {
  def fit(data: PipelineDatasetOut[A]): Pipeline[A, B]
  def fit(data: RDD[A]): Pipeline[A, B]
}

abstract class LabelEstimator[A, B, L] {
  def fit(data: PipelineDatasetOut[A], labels: PipelineDatasetOut[L]): Pipeline[A, B]
  def fit(data: RDD[A], labels: RDD[L]): Pipeline[A, B]
}

object GraphBackedExecution {
  // Combine all the internal graph representations to use the same, merged representation
  def tie(graphBackedExecutions: Seq[GraphBackedExecution], optimizer: Option[Optimizer]): Unit = {
    val emptyGraph = new Graph(Set(), Map(), Map(), Map())
    val (newGraph, sourceMappings, sinkMappings) = graphBackedExecutions.foldLeft(
      emptyGraph,
      Seq[Map[SourceId, SourceId]](),
      Seq[Map[SinkId, SinkId]]()
    ) {
      case ((curGraph, curSourceMappings, curSinkMappings), graphExecution) =>
        val (nextGraph, nextSourceMapping, nextSinkMapping) = curGraph.addGraph(graphExecution.executor.graph)
        (nextGraph, curSourceMappings :+ nextSourceMapping, curSinkMappings :+ nextSinkMapping)
    }

    val newExecutor = new GraphExecutor(graph = newGraph, optimizer = optimizer)
    for (i <- graphBackedExecutions.indices) {
      val execution = graphBackedExecutions(i)
      execution.executor = newExecutor
      execution.sources = execution.sources.map(sourceMappings(i))
      execution.sinks = execution.sinks.map(sinkMappings(i))
    }
  }
}

object Pipeline {

  // If the checkpoint is found, return an output that just reads it from disk.
  // If the checkpoint is not found, return the input data graph w/ an EstimatorOperator just saves to disk added at the end
  def checkpoint[T](data: PipelineDatasetOut[T], checkpointName: String): PipelineDatasetOut[T] = ???
}