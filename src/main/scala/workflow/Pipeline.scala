package workflow

import org.apache.spark.rdd.RDD
import pipelines.Logging

case class Pipeline[A, B](
  nodes: Seq[Node],
  dataDeps: Seq[Seq[Int]],
  fitDeps: Seq[Seq[Int]],
  sink: Int) extends Logging {

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

  private val fitCache: Array[Option[TransformerNode[_]]] = nodes.map(_ => None).toArray

  private def fitEstimator(node: Int): TransformerNode[_] = fitCache(node).getOrElse {
    nodes(node) match {
      case _: DataNode[_] =>
        throw new RuntimeException("Pipeline DAG error: Cannot have a fit dependency on a DataNode")
      case _: TransformerNode[_] =>
        throw new RuntimeException("Pipeline DAG error: Cannot have a data dependency on a Transformer")
      case estimator: EstimatorNode[_] =>
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
        case _: DataNode[_] =>
          throw new RuntimeException("Pipeline DAG error: came across an RDD data dependency when trying to do a single item apply")
        case _: EstimatorNode[_] =>
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
        case _: EstimatorNode[_] =>
          throw new RuntimeException("Pipeline DAG error: Cannot have a data dependency on an Estimator")
      }
    }
  }

  def apply(in: A): B = singleDataEval(sink, in).asInstanceOf[B]

  def apply(in: RDD[A]): RDD[B] = rddDataEval(sink, in).asInstanceOf[RDD[B]]

  def andThen[C](next: Pipeline[B, C]): Pipeline[A, C] = {
    val nodes = this.nodes ++ next.nodes
    val dataDeps = this.dataDeps ++ next.dataDeps.map(_.map {
      x => if (x == Pipeline.SOURCE) this.sink else x + this.nodes.size
    })
    val fitDeps = this.fitDeps ++ next.fitDeps.map(_.map {
      x => if (x == Pipeline.SOURCE) this.sink else x + this.nodes.size
    })
    val sink = next.sink

    Pipeline(nodes, dataDeps, fitDeps, sink)
  }

  def toDOTString: String = {
    val nodeLabels: Seq[String] = "-1 [label='In' shape='Msquare']" +: nodes.zipWithIndex.map {
      case (data: DataNode[_], id)  => s"$id [label='${data.label}' shape='box' style='filled']"
      case (transformer: TransformerNode[_], id) => s"$id [label='${transformer.label}']"
      case (estimator: EstimatorNode[_], id) => s"$id [label='${estimator.label}' shape='diamond']"
    } :+ s"${nodes.size} [label='Out' shape='Msquare']"

    val dataEdges: Seq[String] = dataDeps.zipWithIndex.flatMap {
      case (deps, id) => deps.map(x => s"$x -> $id")
    } :+ s"$sink -> ${nodes.size}"

    val fitEdges: Seq[String] = fitDeps.zipWithIndex.flatMap {
      case (deps, id) => deps.map(x => s"$x -> $id [dir='none' style='dashed']")
    }

    val lines = nodeLabels ++ dataEdges ++ fitEdges
    lines.mkString("digraph pipeline {\n  rankdir=LR;\n  ", "\n  ", "\n}")
  }
}

object Pipeline {
  val SOURCE: Int = -1

  def apply[T](): Pipeline[T, T] = Pipeline(Seq(), Seq(), Seq(), SOURCE)
}