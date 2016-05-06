package workflow.graph

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

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

  def getExecutor: GraphExecutor = executor
  def getGraph: Graph = executor.graph
  def getOptimizer: Option[Optimizer] = executor.optimizer
}

// A lazy representation of a pipeline output
class PipelineDatumOut[T](protected override var executor: GraphExecutor, sink: SinkId) extends GraphBackedExecution {
  protected override var sources = Seq()
  protected override var sinks = Seq(sink)
  def getSink: SinkId = sinks.head

  def get(): T = executor.execute(sinks.head, Map()).asInstanceOf[DatumExpression].get.asInstanceOf[T]
}

// A lazy representation of a pipeline output
class PipelineDatasetOut[T](protected override var executor: GraphExecutor, sink: SinkId) extends GraphBackedExecution {
  protected override var sources = Seq()
  protected override var sinks = Seq(sink)
  def getSink: SinkId = sinks.head

  def get(): RDD[T] = executor.execute(sinks.head, Map()).asInstanceOf[DatasetExpression].get.asInstanceOf[RDD[T]]
}

object PipelineRDDUtils {
  def rddToPipelineDatasetOut[T](rdd: RDD[T]): PipelineDatasetOut[T] = {
    val emptyGraph = Graph(Set(), Map(), Map(), Map())
    val (graphWithDataset, nodeId) = emptyGraph.addNode(new DatasetOperator(rdd), Seq())
    val (graph, sinkId) = graphWithDataset.addSink(nodeId)

    new PipelineDatasetOut[T](new GraphExecutor(graph, None), sinkId)
  }

  def datumToPipelineDatumOut[T](datum: T): PipelineDatumOut[T] = {
    val emptyGraph = Graph(Set(), Map(), Map(), Map())
    val (graphWithDataset, nodeId) = emptyGraph.addNode(new DatumOperator(datum), Seq())
    val (graph, sinkId) = graphWithDataset.addSink(nodeId)

    new PipelineDatumOut[T](new GraphExecutor(graph, None), sinkId)
  }
}

class Pipeline[A, B](protected var executor: GraphExecutor, source: SourceId, sink: SinkId) {
  // These three vars point to the internal representation of this pipeline
  protected var sources = Seq(source)
  protected var sinks = Seq(sink)

  def getSource: SourceId = sources.head
  def getSink: SinkId = sinks.head

  final def apply(datum: A): PipelineDatumOut[B] = apply(PipelineRDDUtils.datumToPipelineDatumOut(datum))

  final def apply(data: RDD[A]): PipelineDatasetOut[B] = apply(PipelineRDDUtils.rddToPipelineDatasetOut(data))

  final def apply(data: PipelineDatasetOut[A]): PipelineDatasetOut[B] = {
    val (newGraph, _, newSinkMapping) = data.getExecutor.graph.connectGraph(executor.graph, Map(getSource -> data.getSink))
    new PipelineDatasetOut[B](new GraphExecutor(newGraph, executor.optimizer), newSinkMapping(getSink))
  }

  final def apply(datum: PipelineDatumOut[A]): PipelineDatumOut[B] = {
    val (newGraph, _, newSinkMapping) = datum.getExecutor.graph.connectGraph(executor.graph, Map(getSource -> datum.getSink))
    new PipelineDatumOut[B](new GraphExecutor(newGraph, executor.optimizer), newSinkMapping(getSink))
  }

  // TODO: Clean up this method
  final def andThen[C](next: Pipeline[B, C]): Pipeline[A, C] = {
    val (newGraph, _, newSinkMappings) = executor.graph.connectGraph(next.executor.graph, Map(next.getSource -> getSink))
    new Pipeline[A, C](new GraphExecutor(newGraph, executor.optimizer), getSource, newSinkMappings(next.getSink))
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

abstract class Transformer[A, B : ClassTag] extends TransformerOperator {
  protected def singleTransform(in: A): B
  protected def batchTransform(in: RDD[A]): RDD[B] = in.map(singleTransform)

  final override private[graph] def singleTransform(inputs: Seq[DatumExpression]): Any = {
    singleTransform(inputs.head.get.asInstanceOf[A])
  }

  final override private[graph] def batchTransform(inputs: Seq[DatasetExpression]): RDD[_] = {
    batchTransform(inputs.head.get.asInstanceOf[RDD[A]])
  }
}


abstract class Estimator[A, B] extends EstimatorOperator {
  def fit(data: RDD[A]): Pipeline[A, B] = fit(PipelineRDDUtils.rddToPipelineDatasetOut(data))
  def fit(data: PipelineDatasetOut[A]): Pipeline[A, B] = {
    val curSink = data.getExecutor.graph.getSinkDependency(data.getSink)
    val (newGraph, nodeId) = data.getExecutor.graph.removeSink(data.getSink).addNode(this, Seq(curSink))
    val (newGraphWithSource, sourceId) = newGraph.addSource()
    val (almostFinalGraph, delegatingId) = newGraphWithSource.addNode(new DelegatingOperator, Seq(nodeId, sourceId))
    val (finalGraph, sinkId) = almostFinalGraph.addSink(delegatingId)

    new Pipeline(new GraphExecutor(finalGraph, data.getExecutor.optimizer), sourceId, sinkId)
  }

  final override private[graph] def fitRDDs(inputs: Seq[DatasetExpression]): TransformerOperator = {
    fitRDD(inputs.head.get.asInstanceOf[RDD[A]])
  }
  protected def fitRDD(data: RDD[A]): Transformer[A, B]
}

abstract class LabelEstimator[A, B, L] extends EstimatorOperator {
  def fit(data: RDD[A], labels: PipelineDatasetOut[L]): Pipeline[A, B] = fit(PipelineRDDUtils.rddToPipelineDatasetOut(data), labels)
  def fit(data: PipelineDatasetOut[A], labels: RDD[L]): Pipeline[A, B] = fit(data, PipelineRDDUtils.rddToPipelineDatasetOut(labels))
  def fit(data: RDD[A], labels: RDD[L]): Pipeline[A, B] = fit(PipelineRDDUtils.rddToPipelineDatasetOut(data), PipelineRDDUtils.rddToPipelineDatasetOut(labels))
  def fit(data: PipelineDatasetOut[A], labels: PipelineDatasetOut[L]): Pipeline[A, B] = {
    val (depGraph, _, labelSinkMapping) = data.getGraph.addGraph(labels.getGraph)
    val dataSink = depGraph.getSinkDependency(data.getSink)
    val labelsSink = depGraph.getSinkDependency(labelSinkMapping(labels.getSink))
    val (newGraph, nodeId) = depGraph
      .removeSink(data.getSink)
      .removeSink(labelSinkMapping(labels.getSink))
      .addNode(this, Seq(dataSink, labelsSink))
    val (newGraphWithSource, sourceId) = newGraph.addSource()
    val (almostFinalGraph, delegatingId) = newGraphWithSource.addNode(new DelegatingOperator, Seq(nodeId, sourceId))
    val (finalGraph, sinkId) = almostFinalGraph.addSink(delegatingId)

    new Pipeline(new GraphExecutor(finalGraph, data.getExecutor.optimizer), sourceId, sinkId)
  }

  final override private[graph] def fitRDDs(inputs: Seq[DatasetExpression]): TransformerOperator = {
    fitRDDs(inputs(0).get.asInstanceOf[RDD[A]], inputs(1).get.asInstanceOf[RDD[L]])
  }
  protected def fitRDDs(data: RDD[A], labels: RDD[L]): Transformer[A, B]

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

case class Identity[T : ClassTag]() extends Transformer[T,T] {
  override protected def singleTransform(in: T): T = in
  override protected def batchTransform(in: RDD[T]): RDD[T] = in
}

case class Checkpointer[T : ClassTag](path: String) extends Estimator[T, T] {
  override protected def fitRDD(data: RDD[T]): Transformer[T, T] = {
    data.saveAsObjectFile(path)
    new Identity[T]
  }
}

object Pipeline {

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
}