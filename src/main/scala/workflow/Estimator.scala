package workflow

import java.io.Serializable

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait EstimatorPipeline[A, B, C, T <: ExposableTransformer[B, C, T]] {
  private[workflow] val nodes: Seq[Node]
  private[workflow] val dataDeps: Seq[Seq[Int]]
  private[workflow] val fitDeps: Seq[Seq[Int]]
  private[workflow] val sink: Int

  def withData(data: RDD[A]): SingleTransformerPipeline[B, C, T] = {
    val label = {
      val className = nodes(sink).getClass.getSimpleName
      if (className endsWith "$") className.dropRight(1) else className
    } + ".fit"

    val newNodes = nodes :+ DataNode(data) :+ new DelegatingTransformer[C](label)
    val newDataDeps = dataDeps.map(_.map(x => if (x == Pipeline.SOURCE) nodes.size else x)) :+ Seq() :+ Seq(Pipeline.SOURCE)
    val newFitDeps = fitDeps :+ Seq() :+ Seq(sink)
    val newSink = newNodes.size - 1

    new ConcreteSingleTransformerPipeline(newNodes, newDataDeps, newFitDeps, newSink)
  }
}

private[workflow] class ConcreteEstimatorPipeline[A, B, C, T <: ExposableTransformer[B, C, T]](
  override val nodes: Seq[Node],
  override val dataDeps: Seq[Seq[Int]],
  override val fitDeps: Seq[Seq[Int]],
  override val sink: Int) extends EstimatorPipeline[A, B, C, T]

/**
 * An estimator has a `fit` method which takes an input and emits a [[Transformer]]
 * @tparam A The type of input this estimator (and the resulting Transformer) takes
 * @tparam B The output type of the node this estimator produces when being fit
 */
abstract class ModelExposingEstimator[A, B, T <: ExposableTransformer[A, B, T]] extends EstimatorNode with EstimatorPipeline[A, A, B, T]  {
  override val nodes: Seq[Node] = Seq(this)
  override val dataDeps: Seq[Seq[Int]] = Seq(Seq(Pipeline.SOURCE))
  override val fitDeps: Seq[Seq[Int]] = Seq(Seq())
  override val sink: Int = 0

  /**
   * An estimator has a `fit` method which emits a [[Transformer]].
   * @param data Input data.
   * @return A [[Transformer]] which can be called on new data.
   */
  protected def fit(data: RDD[A]): T

  override def fit(dependencies: Seq[RDD[_]]): TransformerNode[_] = fit(dependencies.head.asInstanceOf[RDD[A]])
}

/**
 * An estimator has a `fit` method which takes an input and emits a [[Transformer]]
 * @tparam A The type of input this estimator (and the resulting Transformer) takes
 * @tparam B The output type of the node this estimator produces when being fit
 */
abstract class Estimator[A, B : ClassTag] extends ModelExposingEstimator[A, B, Transformer[A, B]]

object Estimator extends Serializable {
  /**
   * This constructor takes a function and returns an estimator. The function must itself return a [[Transformer]].
   *
   * @param node An estimator function. It must return a function.
   * @tparam I Input type of the estimator and the transformer it produces.
   * @tparam O Output type of the estimator and the transformer it produces.
   * @return An Estimator which can be applied to new data.
   */
  def apply[I, O : ClassTag](node: RDD[I] => Transformer[I, O]): Estimator[I, O] = new Estimator[I, O] {
    override def fit(rdd: RDD[I]): Transformer[I, O] = node.apply(rdd)
  }
}
