package pipelines

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

sealed abstract class Node[A, B] extends Serializable {

  /**
   * Chains an estimator onto this Transformer, producing a new estimator that when fit on same input type as
   * this transformer, chains this transformer with the transformer output by the original estimator.
   * @param est The estimator to chain onto the Transformer
   * @return  The output estimator
   */
  def thenEstimator[C : ClassTag](est: Estimator[B, C]): Estimator[A, C] = new NodeAndEstimator(this, est)

  /**
   * Chains a Label Estimator onto this Transformer, producing a new Label Estimator that when fit on same input
   * type as this transformer, chains this transformer with the transformer output by the original Label Estimator.
   * @param est The label estimator to chain onto the Transformer
   * @return  The output label estimator
   */
  def thenLabelEstimator[C : ClassTag, L : ClassTag](est: LabelEstimator[B, C, L]): LabelEstimator[A, C, L] = new NodeAndLabelEstimator(this, est)

  /**
   * Chains another Transformer onto this one, producing a new Transformer that applies both in sequence
   * @param next The Transformer to attach to the end of this one
   * @return The output Transformer
   */
  def then[C : ClassTag](next: Node[B, C]): Pipeline[A, C] = {
    Pipeline(this.rewrite ++ next.rewrite)
  }

  /**
   * Chains a method, producing a new Transformer that applies the method to each
   * output item after applying this Transformer first.
   * @param next The method to apply to each item output by this transformer
   * @return The output Transformer
   */
  def thenFunction[C : ClassTag](next: B => C): Pipeline[A, C] = this.then(Transformer(next))

  def rewrite: Seq[Node[_, _]]
  def fit(): Transformer[A, B]
  def canElevate: Boolean
}

case class Pipeline[A, B : ClassTag](nodes: Seq[Node[_, _]]) extends Node[A, B] {
  override def then[C : ClassTag](next: Node[B, C]): Pipeline[A, C] = {
    Pipeline(nodes.flatMap(_.rewrite) ++ next.rewrite)
  }

  def fit(): PipelineModel[A, B] = {
    var transformers: Seq[Transformer[_, _]] = Seq()
    nodes.flatMap(_.rewrite).foreach { node =>
      val nextTransformer = node match {
        case EstimatorWithData(est, data) => {
          est.unsafeFit(PipelineModel(transformers).unsafeRDDApply(data))
        }
        case LabelEstimatorWithData(est, data, labels) => {
          est.unsafeFit(PipelineModel(transformers).unsafeRDDApply(data), labels)
        }
        case _ => node.fit()
      }
      transformers = transformers ++ nextTransformer.rewrite
    }

    PipelineModel[A, B](transformers)
  }

  override def rewrite: Seq[Node[_, _]] = {
    val rewrittenNodes = nodes.flatMap(_.rewrite)
    if (rewrittenNodes.forall(_.canElevate)) {
      rewrittenNodes
    } else {
      Seq(Pipeline(nodes.flatMap(_.rewrite)))
    }
  }

  def canElevate = true
}

class NodeAndEstimator[A, B, C : ClassTag](node: Node[A, B], estimator: Estimator[B, C]) extends Estimator[A, C] {
  override def withData(data: RDD[A]) = {
    node then EstimatorWithData(estimator, data)
  }

  def fit(data: RDD[A]): Transformer[A, C] = withData(data).fit()
}

class NodeAndLabelEstimator[A, B, C : ClassTag, L](node: Node[A, B], estimator: LabelEstimator[B, C, L]) extends LabelEstimator[A, C, L] {
  override def withData(data: RDD[A], labels: RDD[L]) = {
    node then LabelEstimatorWithData(estimator, data, labels)
  }

  def fit(data: RDD[A], labels: RDD[L]): Transformer[A, C] = withData(data, labels).fit()
}

case class EstimatorWithData[A, B](estimator: Estimator[A, B], data: RDD[_]) extends Node[A, B] {
  def fit(): Transformer[A, B] = estimator.unsafeFit(data)

  def rewrite: Seq[Node[_, _]] = Seq(this)

  def canElevate: Boolean = false
}

case class LabelEstimatorWithData[A, B, L](estimator: LabelEstimator[A, B, L], data: RDD[_], labels: RDD[_]) extends Node[A, B] {
  def fit(): Transformer[A, B] = estimator.unsafeFit(data, labels)

  def rewrite: Seq[Node[_, _]] = Seq(this)

  def canElevate: Boolean = false
}

/**
 * Transformers are functions that may be applied both to single input items and to RDDs of input items.
 * They may be chained together, along with [[Estimator]]s and [[LabelEstimator]]s, to produce complex
 * pipelines.
 *
 * @tparam A input item type the transformer takes
 * @tparam B output item type the transformer produces
 */
abstract class Transformer[A, B : ClassTag] extends Node[A, B] {
  /**
   * Apply this Transformer to an RDD of input items
   * @param in The bulk RDD input to pass into this transformer
   * @return The bulk RDD output for the given input
   */
  def apply(in: RDD[A]): RDD[B] = in.map(apply)

  /**
   * Apply this Transformer to a single input item
   * @param in  The input item to pass into this transformer
   * @return  The output value
   */
  def apply(in: A): B

  final def unsafeSingleApply(x: Any) = apply(x.asInstanceOf[A])
  final def unsafeRDDApply(x: Any) = apply(x.asInstanceOf[RDD[A]])

  def fit(): Transformer[A, B] = this
  def rewrite: Seq[Transformer[_, _]] = Seq(this)
  def canElevate: Boolean = true
}

object Transformer {
  /**
   * This constructor takes a function and returns a Transformer that maps it over the input RDD
   * @param f The function to apply to every item in the RDD being transformed
   * @tparam I input type of the transformer
   * @tparam O output type of the transformer
   * @return Transformer that applies the given function to all items in the RDD
   */
  def apply[I, O : ClassTag](f: I => O): Transformer[I, O] = new Transformer[I, O] {
    override def apply(in: RDD[I]): RDD[O] = in.map(f)
    override def apply(in: I): O = f(in)
  }
}

case class PipelineModel[A, B : ClassTag](transformers: Seq[Transformer[_, _]]) extends Transformer[A, B] {
  def apply(in: A): B = {
    var x: Any = in
    transformers.foreach { transformer =>
      x = transformer.unsafeSingleApply(x)
    }
    x.asInstanceOf[B]
  }

  override def apply(in: RDD[A]): RDD[B] = {
    var x: Any = in
    transformers.foreach { transformer =>
      x = transformer.unsafeRDDApply(x)
    }
    x.asInstanceOf[RDD[B]]
  }

  override def rewrite: Seq[Transformer[_, _]] = transformers.flatMap(_.rewrite)
}
