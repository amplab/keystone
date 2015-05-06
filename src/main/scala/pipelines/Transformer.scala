package pipelines

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


/**
 * Transformers are functions that may be applied both to single input items and to RDDs of input items.
 * They may be chained together, along with [[Estimator]]s and [[LabelEstimator]]s, to produce complex
 * pipelines.
 *
 * @tparam A input item type the transformer takes
 * @tparam B output item type the transformer produces
 */
abstract class Transformer[A, B : ClassTag] extends Serializable {
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

  /**
   * Chains an estimator onto this Transformer, producing a new estimator that when fit on same input type as
   * this transformer, chains this transformer with the transformer output by the original estimator.
   * @param est The estimator to chain onto the Transformer
   * @return  The output estimator
   */
  def thenEstimator[C : ClassTag](est: Estimator[B, C]): Estimator[A, C] = Estimator((a: RDD[A]) => this then est.fit(this(a)))

  /**
   * Chains a Label Estimator onto this Transformer, producing a new Label Estimator that when fit on same input
   * type as this transformer, chains this transformer with the transformer output by the original Label Estimator.
   * @param est The label estimator to chain onto the Transformer
   * @return  The output label estimator
   */
  def thenLabelEstimator[C : ClassTag, L : ClassTag](est: LabelEstimator[B, C, L]): LabelEstimator[A, C, L] = LabelEstimator((a: RDD[A], l: RDD[L]) => this then est.fit(this(a), l))

  /**
   * Chains another Transformer onto this one, producing a new Transformer that applies both in sequence
   * @param next The Transformer to attach to the end of this one
   * @return The output Transformer
   */
  def then[C : ClassTag](next: Transformer[B, C]): Transformer[A, C] = {
    val first = this
    val second = next
    new Transformer[A, C] {
      override def apply(in: RDD[A]): RDD[C] = second(first(in))
      override def apply(in: A): C = second(first(in))
    }
  }

  /**
   * Chains a method, producing a new Transformer that applies the method to each
   * output item after applying this Transformer first.
   * @param next The method to apply to each item output by this transformer
   * @return The output Transformer
   */
  def then[C : ClassTag](next: B => C): Transformer[A, C] = this.then(Transformer(next))

  /**
   * Casts the output of this transformer to RDD[U] if each item can be cast to type U
   * @tparam C  The type the cast RDD should hold
   */
  def to[C : ClassTag](implicit ev: B => C): Transformer[A, C] = this.then(ev)

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
