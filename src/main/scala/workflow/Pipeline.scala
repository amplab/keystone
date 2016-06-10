package workflow

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * A Pipeline takes data as input (single item or an RDD), and outputs some transformation
 * of that data. Internally, a Pipeline contains a [[GraphExecutor]], a specified source, and a specified sink.
 * When a pipeline is applied to data it produces a [[PipelineResult]], in the form of either a [[PipelineDataset]]
 * or a [[PipelineDatum]]. These are lazy wrappers around the scheduled execution under the hood,
 * and when their values are accessed the underlying [[Graph]] will be executed.
 *
 * Warning: Not thread-safe!
 *
 * @param executor The [[GraphExecutor]] underlying the Pipeline execution.
 * @param source The SourceId of the Pipeline
 * @param sink The SinkId of the Pipeline
 * @tparam A type of the data this Pipeline expects as input
 * @tparam B type of the data this Pipeline outputs
 */
class Pipeline[A, B] private[workflow] (
  private[workflow] val executor: GraphExecutor,
  private[workflow] val source: SourceId,
  private[workflow] val sink: SinkId) extends Chainable[A, B] {

  def toPipeline: Pipeline[A, B] = this

  /**
   * Fit all Estimators in this pipeline to produce a [[FittedPipeline]].
   * It is logically equivalent, but only contains Transformers in the underlying graph.
   * Applying the FittedPipeline to new data does not trigger any new optimization or estimator fitting.
   *
   * It is also serializable and may be written to and from disk.
   *
   * @return the fitted version of this Pipeline.
   */
  final def fit(): FittedPipeline[A, B] = {
    val optimizedGraph = PipelineEnv.getOrCreate.getOptimizer.execute(executor.graph, Map())._1

    val estFittingExecutor = new GraphExecutor(optimizedGraph, optimize = false)
    val delegatingNodes = optimizedGraph.operators.collect {
      case (node, _: DelegatingOperator) => node
    }

    val graphWithFitEstimators = delegatingNodes.foldLeft(optimizedGraph) {
      case (curGraph, node) => {
        val deps = optimizedGraph.getDependencies(node)
        val estimatorDep = deps.head
        val transformer = estFittingExecutor.execute(estimatorDep).get.asInstanceOf[TransformerOperator]

        curGraph.setOperator(node, transformer).setDependencies(node, deps.tail)
      }
    }

    val newGraph = UnusedBranchRemovalRule.apply(graphWithFitEstimators, Map())._1

    val transformerGraph = TransformerGraph(
      newGraph.sources,
      newGraph.sinkDependencies,
      newGraph.operators.map(op => (op._1, op._2.asInstanceOf[TransformerOperator])),
      newGraph.dependencies)

    new FittedPipeline[A, B](transformerGraph, source, sink)
  }

  /**
   * Lazily apply the pipeline to a single datum.
   *
   * @return A lazy wrapper around the result of passing the datum through the pipeline.
   */
  final def apply(datum: A): PipelineDatum[B] = {
    apply(PipelineDatum(datum))
  }

  /**
   * Lazily apply the pipeline to a dataset.
   *
   * @return A lazy wrapper around the result of passing the dataset through the pipeline.
   */
  final def apply(data: RDD[A]): PipelineDataset[B] = {
    apply(PipelineDataset(data))
  }

  /**
   * Lazily apply the pipeline to the lazy output of a different pipeline given an initial dataset.
   * If the previous pipeline has already been fit, it will not need to be fit again.
   *
   * @return A lazy wrapper around the result of passing lazy output from a different pipeline through this pipeline.
   */
  final def apply(data: PipelineDataset[A]): PipelineDataset[B] = {
    val (newGraph, _, _, sinkMapping) =
      data.executor.graph.connectGraph(executor.graph, Map(source -> data.sink))

    new PipelineDataset[B](new GraphExecutor(newGraph, executor.optimize), sinkMapping(sink))
  }

  /**
   * Lazily apply the pipeline to the lazy output of a different pipeline given an initial datum.
   * If the previous pipeline has already been fit, it will not need to be fit again.
   *
   * @return A lazy wrapper around the result of passing lazy output from a different pipeline through this pipeline.
   */
  final def apply(datum: PipelineDatum[A]): PipelineDatum[B] = {
    val (newGraph, _, _, sinkMapping) =
      datum.executor.graph.connectGraph(executor.graph, Map(source -> datum.sink))

    new PipelineDatum[B](new GraphExecutor(newGraph, executor.optimize), sinkMapping(sink))
  }
}

object Pipeline {
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
    val (graphWithGather, gatherNode) = graphWithAllBranches.addNode(new GatherTransformerOperator[B], branchSinks)
    val (newGraph, sink) = graphWithGather.addSink(gatherNode)

    // We construct & return the new gathered pipeline
    val executor = new GraphExecutor(newGraph)
    new Pipeline[A, Seq[B]](executor, source, sink)
  }
}
