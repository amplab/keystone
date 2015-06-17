package workflow

import org.apache.spark.rdd.RDD
import pipelines.Logging

trait Pipeline[A, B] {
  private[workflow] val nodes: Seq[Node]
  private[workflow] val dataDeps: Seq[Seq[Int]]
  private[workflow] val fitDeps: Seq[Seq[Int]]
  private[workflow] val sink: Int

  validate()

  /** TODO: require that
    - nodes.size = dataDeps.size == fitDeps.size
    - there is a sink
    - there is a data path from sink to in
    - there are no fit paths from sink to in
    - only data nodes may be sources (no deps)
    - data nodes must have no deps
    - estimators may not have fit deps, must have data deps
    - transformers must have data deps, may or may not have fit deps
    - fit deps may only point to estimators
    */
  private def validate(): Unit = {

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

  final def andThenEstimate[C, D](est: EstimatorPipeline[B, C, D]): EstimatorPipeline[A, C, D] = {
    val nodes = this.nodes ++ est.nodes
    val dataDeps = this.dataDeps ++ est.dataDeps.map(_.map {
      x => if (x == Pipeline.SOURCE) this.sink else x + this.nodes.size
    })
    val fitDeps = this.fitDeps ++ est.fitDeps.map(_.map {
      x => if (x == Pipeline.SOURCE) this.sink else x + this.nodes.size
    })
    val sink = est.sink + this.nodes.size

    new ConcreteEstimatorPipeline(nodes, dataDeps, fitDeps, sink)
  }

  final def andThenLabelEstimate[C, D, L](est: LabelEstimatorPipeline[B, C, D, L]): LabelEstimatorPipeline[A, C, D, L] = {
    val nodes = this.nodes ++ est.nodes
    val dataDeps = this.dataDeps ++ est.dataDeps.map(_.map {
      case Pipeline.SOURCE => this.sink
      case LabelEstimatorPipeline.LABEL_SOURCE => LabelEstimatorPipeline.LABEL_SOURCE
      case x => x + this.nodes.size
    })
    val fitDeps = this.fitDeps ++ est.fitDeps.map(_.map {
      x => if (x == Pipeline.SOURCE) this.sink else x + this.nodes.size
    })
    val sink = est.sink + this.nodes.size

    new ConcreteLabelEstimatorPipeline(nodes, dataDeps, fitDeps, sink)
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

  def apply[T](): Pipeline[T, T] = new ConcretePipeline(Seq(), Seq(), Seq(), SOURCE)
  private[workflow] def apply[A, B](nodes: Seq[Node], dataDeps: Seq[Seq[Int]], fitDeps: Seq[Seq[Int]], sink: Int): Pipeline[A, B] = new ConcretePipeline(nodes, dataDeps, fitDeps, sink)
}

private[workflow] class ConcretePipeline[A, B](
  override val nodes: Seq[Node],
  override val dataDeps: Seq[Seq[Int]],
  override val fitDeps: Seq[Seq[Int]],
  override val sink: Int) extends Pipeline[A, B] with Logging {

  private val fitCache: Array[Option[TransformerNode[_]]] = nodes.map(_ => None).toArray

  private def fitEstimator(node: Int): TransformerNode[_] = fitCache(node).getOrElse {
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

  private def singleDataEval(node: Int, in: A): Any = {
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

  private def rddDataEval(node: Int, in: RDD[A]): RDD[_] = {
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
