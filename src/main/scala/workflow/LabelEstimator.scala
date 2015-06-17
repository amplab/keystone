package workflow

import java.io.Serializable

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait LabelEstimatorPipeline[A, B, C, L] {
  private[workflow] val nodes: Seq[Node]
  private[workflow] val dataDeps: Seq[Seq[Int]]
  private[workflow] val fitDeps: Seq[Seq[Int]]
  private[workflow] val sink: Int

  def withData(data: RDD[A], labels: RDD[L]): Pipeline[B, C] = {
    val label = {
      val className = nodes(sink).getClass.getSimpleName
      if (className endsWith "$") className.dropRight(1) else className
    } + ".fit"

    val newNodes = nodes :+ DataNode(data) :+ DataNode(labels) :+ new DelegatingTransformer[C](label)
    val newDataDeps = dataDeps.map(_.map {
      case Pipeline.SOURCE => nodes.size
      case LabelEstimatorPipeline.LABEL_SOURCE => nodes.size + 1
      case x => x
    }) :+ Seq() :+ Seq() :+ Seq(Pipeline.SOURCE)
    val newFitDeps = fitDeps :+ Seq() :+ Seq() :+ Seq(sink)
    val newSink = newNodes.size - 1

    Pipeline(newNodes, newDataDeps, newFitDeps, newSink)
  }
}

private[workflow] class ConcreteLabelEstimatorPipeline[A, B, C, L](
  override val nodes: Seq[Node],
  override val dataDeps: Seq[Seq[Int]],
  override val fitDeps: Seq[Seq[Int]],
  override val sink: Int) extends LabelEstimatorPipeline[A, B, C, L]

object LabelEstimatorPipeline {
  val LABEL_SOURCE: Int = -2
}

/**
 * A label estimator has a `fit` method which takes input data & labels and emits a [[Transformer]]
 * @tparam A The type of the input data
 * @tparam B The type of output of the emitted transformer
 * @tparam L The type of label this node expects
 */
abstract class LabelEstimator[A, B : ClassTag, L] extends EstimatorNode with LabelEstimatorPipeline[A, A, B, L]  {
  override val nodes: Seq[Node] = Seq(this)
  override val dataDeps: Seq[Seq[Int]] = Seq(Seq(Pipeline.SOURCE, LabelEstimatorPipeline.LABEL_SOURCE))
  override val fitDeps: Seq[Seq[Int]] = Seq(Seq())
  override val sink: Int = 0

  /**
   * A LabelEstimator estimator is an estimator which expects labeled data.
   * @param data Input data.
   * @param labels Input labels.
   * @return A [[Transformer]] which can be called on new data.
   */
  protected def fit(data: RDD[A], labels: RDD[L]): Transformer[A, B]

  override def fit(dependencies: Seq[RDD[_]]): TransformerNode[_] = fit(dependencies(0).asInstanceOf[RDD[A]], dependencies(1).asInstanceOf[RDD[L]])
}

object LabelEstimator extends Serializable {
  /**
   * This constructor takes a function which expects labeled data and returns an estimator.
   * The function must itself return a transformer.
   *
   * @param node An estimator function. It must take labels and data and return a function from data to output.
   * @tparam I Input type of the labeled estimator and the transformer it produces.
   * @tparam O Output type of the estimator and the transformer it produces.
   * @tparam L Label type of the estimator.
   * @return An Estimator which can be applied to new labeled data.
   */
  def apply[I, O : ClassTag, L](node: (RDD[I], RDD[L]) => Transformer[I, O]): LabelEstimator[I, O, L] = new LabelEstimator[I, O, L] {
    override def fit(v1: RDD[I], v2: RDD[L]): Transformer[I, O] = node(v1, v2)
  }
}
