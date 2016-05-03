package workflow.graph

import org.apache.spark.rdd.RDD

class GraphExecutor(private val graph: Graph, private val optimizer: Optimizer) {
  private lazy val optimizedGraph: Graph = optimizer.execute(graph)
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
class PipelineDatumOut[T] extends GraphBackedExecution {
  protected var sources = Seq()
  protected var sinks = Seq()
  def get(): T = executor.execute(sinks.head, Map()).asInstanceOf[DatumExpression].get.asInstanceOf[T]
}

// A lazy representation of a pipeline output
class PipelineDatasetOut[T] extends GraphBackedExecution {
  def get(): RDD[T] = executor.execute(sinks.head, Map()).asInstanceOf[DatasetExpression].get.asInstanceOf[RDD[T]]
}

abstract class Pipeline[A, B] {
  // These three vars point to the internal representation of this pipeline

  def apply(datum: A): PipelineDatumOut[B]
  def apply(data: RDD[A]): PipelineDatasetOut[B]
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

object Pipeline {
  // Combine all the internal graph representations to use the same, merged representation
  def tie(outs: GraphBackedExecution*): Unit = ???

  // If the checkpoint is found, return an output that just reads it from disk.
  // If the checkpoint is not found, return the input data graph w/ an EstimatorOperator just saves to disk added at the end
  def checkpoint[T](data: PipelineDatasetOut[T], checkpointName: String): PipelineDatasetOut[T] = ???
}