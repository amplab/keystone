package workflow

import org.apache.spark.rdd.RDD
import pipelines.Logging

import scala.reflect.ClassTag

trait Pipeline[A, B] {
  private[workflow] val nodes: Seq[Node]
  private[workflow] val dataDeps: Seq[Seq[Int]]
  private[workflow] val fitDeps: Seq[Seq[Int]]
  private[workflow] val sink: Int

  /** validates (returns an exception if false) that
    - nodes.size = dataDeps.size == fitDeps.size

    - there is a valid sink
    - data nodes must have no deps
    - estimators may not have fit deps, must have data deps
    - transformers must have data deps, allowed to have fit deps

    - data deps may not point at estimators
    - fit deps may only point to estimators
    */
  private[workflow] def validate(): Unit = {
    require(nodes.size == dataDeps.size && nodes.size == fitDeps.size, "nodes.size must equal dataDeps.size and fitDeps.size")
    require(sink == Pipeline.SOURCE || (sink >= 0 && sink < nodes.size), "Sink must point at a valid node")

    val nodeTuples = nodes.zip(dataDeps).zip(fitDeps).map(x => (x._1._1, x._1._2, x._2))
    require(nodeTuples.forall(x => if (x._1.isInstanceOf[DataNode] && (x._2.nonEmpty || x._3.nonEmpty)) false else true), "DataNodes may not have dependencies")
    require(nodeTuples.forall(x => if (x._1.isInstanceOf[EstimatorNode] && x._3.nonEmpty) false else true), "Estimators may not have fit dependencies")
    require(nodeTuples.forall(x => if (x._1.isInstanceOf[EstimatorNode] && x._2.isEmpty) false else true), "Estimators must have data dependencies")
    require(nodeTuples.forall(x => if (x._1.isInstanceOf[TransformerNode[_]] && x._2.isEmpty) false else true), "Transformers must have data dependencies")

    require(dataDeps.forall(_.forall(x => !nodes(x).isInstanceOf[EstimatorNode])), "Data dependencies may not point at Estimators")
    require(fitDeps.forall(_.forall(x => nodes(x).isInstanceOf[EstimatorNode])), "Fit dependencies may only point at Estimators")

    /*TODO: Validate that
    - there is a data path from sink to in
    - there are no fit paths from sink to in
    */
  }

  def apply(in: A): B

  def apply(in: RDD[A]): RDD[B]

  final def andThen[C](next: Pipeline[B, C]): Pipeline[A, C] = {
    val nodes = this.nodes ++ next.nodes
    val dataDeps = this.dataDeps ++ next.dataDeps.map(_.map {
      x => if (x == Pipeline.SOURCE) this.sink else x + this.nodes.size
    })
    val fitDeps = this.fitDeps ++ next.fitDeps.map(_.map {
      x => if (x == Pipeline.SOURCE) this.sink else x + this.nodes.size
    })
    val sink = next.sink + this.nodes.size

    Pipeline(nodes, dataDeps, fitDeps, sink)
  }

  final def andThen[C](est: Estimator[B, C], data: RDD[A]): PipelineWithFittedTransformer[A, B, C] = {
    val transformerLabel = est.label + ".fit"

    val newNodes = this.nodes :+ est :+ DataNode(data) :+ new DelegatingTransformer[C](transformerLabel)
    val newDataDeps = this.dataDeps.map(_.map {
      x => if (x == Pipeline.SOURCE) this.nodes.size + 1 else x
    }) :+ Seq(this.sink) :+ Seq() :+ Seq(Pipeline.SOURCE)
    val newFitDeps = this.fitDeps :+ Seq() :+ Seq() :+ Seq(this.nodes.size)
    val newSink = newNodes.size - 1

    val fittedTransformer = Pipeline[B, C](newNodes, newDataDeps, newFitDeps, newSink)
    val totalOut = this andThen fittedTransformer
    new PipelineWithFittedTransformer(totalOut.nodes, totalOut.dataDeps, totalOut.fitDeps, totalOut.sink, fittedTransformer)
  }

  final def andThen[C, L](est: LabelEstimator[B, C, L], data: RDD[A], labels: RDD[L]): PipelineWithFittedTransformer[A, B, C] = {
    val transformerLabel = est.label + ".fit"

    val newNodes = this.nodes :+ est :+ DataNode(data) :+ DataNode(labels) :+ new DelegatingTransformer[C](transformerLabel)
    val newDataDeps = this.dataDeps.map(_.map {
      x => if (x == Pipeline.SOURCE) this.nodes.size + 1 else x
    }) :+ Seq(this.sink, this.nodes.size + 2) :+ Seq() :+ Seq() :+ Seq(Pipeline.SOURCE)
    val newFitDeps = this.fitDeps :+ Seq() :+ Seq() :+ Seq() :+ Seq(this.nodes.size)
    val newSink = newNodes.size - 1

    val fittedTransformer = Pipeline[B, C](newNodes, newDataDeps, newFitDeps, newSink)
    val totalOut = this andThen fittedTransformer
    new PipelineWithFittedTransformer(totalOut.nodes, totalOut.dataDeps, totalOut.fitDeps, totalOut.sink, fittedTransformer)
  }


  final def toDOTString: String = {
    val nodeLabels: Seq[String] = "-1 [label=\"In\" shape=\"Msquare\"]" +: nodes.zipWithIndex.map {
      case (data: DataNode, id)  => s"$id [label=${'"' + data.label + '"'} shape=${"\"box\""} style=${"\"filled\""}]"
      case (transformer: TransformerNode[_], id) => s"$id [label=${'"' + transformer.label + '"'}]"
      case (estimator: EstimatorNode, id) => s"$id [label=${'"' + estimator.label + '"'} shape=${"\"box\""}]"
    } :+ s"${nodes.size} [label=${"\"Out\""} shape=${"\"Msquare\""}]"

    val dataEdges: Seq[String] = dataDeps.zipWithIndex.flatMap {
      case (deps, id) => deps.map(x => s"$x -> $id")
    } :+ s"$sink -> ${nodes.size}"

    val fitEdges: Seq[String] = fitDeps.zipWithIndex.flatMap {
      case (deps, id) => deps.map(x => s"$x -> $id [dir=${"\"none\""} style=${"\"dashed\""}]")
    }

    val ranks = fitDeps.zipWithIndex.flatMap {
      case (deps, id) => deps.map(x => s"{rank=same; $x $id}")
    }

    val lines = nodeLabels ++ dataEdges ++ fitEdges ++ ranks
    lines.mkString("digraph pipeline {\n  rankdir=LR;\n  ", "\n  ", "\n}")
  }

  final private[workflow] def planEquals(pipeline: Pipeline[A, B]): Boolean = {
    this.eq(pipeline) || (
        (nodes == pipeline.nodes) &&
        (dataDeps == pipeline.dataDeps) &&
        (fitDeps == pipeline.fitDeps) &&
        (sink == pipeline.sink))
  }
}

object Pipeline {
  val SOURCE: Int = -1

  /**
   * Constructs the Identity pipeline
   * @tparam T The type of input to take
   */
  def apply[T](): Pipeline[T, T] = new ConcretePipeline(Seq(), Seq(), Seq(), SOURCE)
  private[workflow] def apply[A, B](nodes: Seq[Node], dataDeps: Seq[Seq[Int]], fitDeps: Seq[Seq[Int]], sink: Int): Pipeline[A, B] = new ConcretePipeline(nodes, dataDeps, fitDeps, sink)

  /**
   * Produces a pipeline that when given an input,
   * combines the outputs of all its branches when executed on that input into a single Seq (in order)
   * @param branches The pipelines whose outputs should be combined into a Seq
   */
  def gather[A, B : ClassTag](branches: Seq[Pipeline[A, B]]): Pipeline[A, Seq[B]] = {
    // attach a value per branch to offset all existing node ids by.
    val branchesWithNodeOffsets = branches.scanLeft(0)(_ + _.nodes.size).zip(branches)

    val newNodes = branches.map(_.nodes).reduceLeft(_ ++ _) :+ new GatherTransformer[B]

    val newDataDeps = branchesWithNodeOffsets.map { case (offset, branch) =>
      val dataDeps = branch.dataDeps
      dataDeps.map(_.map(x => if (x == Pipeline.SOURCE) Pipeline.SOURCE else x + offset))
    }.reduceLeft(_ ++ _) :+  branchesWithNodeOffsets.map { case (offset, branch) =>
      val sink = branch.sink
      if (sink == Pipeline.SOURCE) Pipeline.SOURCE else sink + offset
    }

    val newFitDeps = branchesWithNodeOffsets.map { case (offset, branch) =>
      val fitDeps = branch.fitDeps
      fitDeps.map(_.map(x => if (x == Pipeline.SOURCE) Pipeline.SOURCE else x + offset))
    }.reduceLeft(_ ++ _) :+  Seq()

    val newSink = newNodes.size - 1
    Pipeline(newNodes, newDataDeps, newFitDeps, newSink)
  }

}

/**
 * An implementation of [[Pipeline]] that explicitly specifies the Pipline DAG as vals,
 * With and apply method that works via traversing the DAG graph and executing it.
 */
private[workflow] class ConcretePipeline[A, B](
  private[workflow] override val nodes: Seq[Node],
  private[workflow] override val dataDeps: Seq[Seq[Int]],
  private[workflow] override val fitDeps: Seq[Seq[Int]],
  private[workflow] override val sink: Int) extends Pipeline[A, B] with Logging {

  private val fitCache: Array[Option[TransformerNode[_]]] = nodes.map(_ => None).toArray

  final private[workflow] def fitEstimator(node: Int): TransformerNode[_] = fitCache(node).getOrElse {
    nodes(node) match {
      case _: DataNode =>
        throw new RuntimeException("Pipeline DAG error: Cannot have a fit dependency on a DataNode")
      case _: TransformerNode[_] =>
        throw new RuntimeException("Pipeline DAG error: Cannot have a data dependency on a Transformer")
      case estimator: EstimatorNode =>
        val nodeDataDeps = dataDeps(node).map(x => rddDataEval(x, null))
        logInfo(s"Fitting '${estimator.label}' [$node]")
        val fitOut = estimator.fit(nodeDataDeps)
        fitCache(node) = Some(fitOut)
        logInfo(s"Finished fitting '${estimator.label}' [$node]")
        fitOut
    }
  }

  final private[workflow] def singleDataEval(node: Int, in: A): Any = {
    if (node == Pipeline.SOURCE) {
      in
    } else {
      nodes(node) match {
        case transformer: TransformerNode[_] =>
          val nodeFitDeps = fitDeps(node).map(fitEstimator)
          val nodeDataDeps = dataDeps(node).map(x => singleDataEval(x, in))
          transformer.transform(nodeDataDeps, nodeFitDeps)
        case _: DataNode =>
          throw new RuntimeException("Pipeline DAG error: came across an RDD data dependency when trying to do a single item apply")
        case _: EstimatorNode =>
          throw new RuntimeException("Pipeline DAG error: Cannot have a data dependency on an Estimator")
      }
    }
  }

  final private[workflow] def rddDataEval(node: Int, in: RDD[A]): RDD[_] = {
    if (node == Pipeline.SOURCE) {
      in
    } else {
      nodes(node) match {
        case DataNode(rdd) => rdd
        case transformer: TransformerNode[_] =>
          val nodeFitDeps = fitDeps(node).map(fitEstimator)
          val nodeDataDeps = dataDeps(node).map(x => rddDataEval(x, in))
          transformer.transformRDD(nodeDataDeps, nodeFitDeps)
        case _: EstimatorNode =>
          throw new RuntimeException("Pipeline DAG error: Cannot have a data dependency on an Estimator")
      }
    }
  }

  override def apply(in: A): B = singleDataEval(sink, in).asInstanceOf[B]

  override def apply(in: RDD[A]): RDD[B] = rddDataEval(sink, in).asInstanceOf[RDD[B]]
}

class PipelineWithFittedTransformer[A, B, C] private[workflow] (
    nodes: Seq[Node],
    dataDeps: Seq[Seq[Int]],
    fitDeps: Seq[Seq[Int]],
    sink: Int,
    val fittedTransformer: Pipeline[B, C])
    extends ConcretePipeline[A, C](nodes, dataDeps, fitDeps, sink)
