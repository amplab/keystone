package pipelines

import org.apache.spark.rdd.RDD
import pipelines.Pipelines.PipelineNode

import scala.reflect.ClassTag

/**
 * Helper object contains fundamental tyep definition. A node is a unary function.
 */
object Pipelines {
  type PipelineNode[Input, Output] = (Input => Output)
}

/**
 * A pipeline stage is a single stage of a pipeline.
 *
 * @tparam I Input type.
 * @tparam O Output type.
 */
trait PipelineStage[I, O]

/**
 * A Pipeline consists of at least two stages (even if one is identity.)
 *
 * @param initStage Initial stage.
 * @param nextStage Second Stage.
 * @tparam A Input type.
 * @tparam B Intermediate type.
 * @tparam C Output type.
 * @tparam L Label type.
 */
class Pipeline[A : ClassTag, B : ClassTag, C : ClassTag, L] private (
    initStage: PipelineStage[A, B],
    nextStage: PipelineStage[B, C]) extends SupervisedEstimator[A, C, L] {

  /**
   * Adds a transformer to the end of the pipeline.
   *
   * @param stage New transformer.
   * @tparam D Transformer output type.
   * @return A new pipeline with the transformer appended.
   */
  def addStage[D : ClassTag](stage: Transformer[C, D]): Pipeline[A, C, D, L] = {
    new Pipeline[A, C, D, L](this, stage)
  }

  /**
   * Adds an unsupervised estimator to the end of the pipeline.
   * THe supervised estimator will train with the output of the current pipeline.
   *
   * @param stage New unsupervised estimator.
   * @tparam D Unsupervised estimator output type.
   * @return A new pipeline with the estimator appended.
   */
  def addStage[D : ClassTag](stage: UnsupervisedEstimator[C, D]): Pipeline[A, C, D, L] = {
    new Pipeline[A, C, D, L](this, stage)
  }

  /**
   * Adds a supervised estimator to the end of the pipeline.
   * The supervised estimator will train with the output of the current pipeline.
   *
   * @param stage New supervised estimator.
   * @tparam D Supervised estimator output type.
   * @return A new pipeline with the estimator appended.
   */
  def addStage[D : ClassTag](stage: SupervisedEstimator[C, D, L]): Pipeline[A, C, D, L] = {
    new Pipeline[A, C, D, L](this, stage)
  }

  /**
   * Adds a new unsupervised estimator with input data to the end of the pipeline.
   *
   * @param stage New unsupervised estimator.
   * @param data Data to train the unsupervised estimator.
   * @tparam D Output type of the unsupervised estimator.
   * @return A new pipeline with the estimator appended.
   */
  def addStage[D : ClassTag](stage: UnsupervisedEstimator[C, D], data: RDD[C]): Pipeline[A, C, D, L] = {
    new Pipeline[A, C, D, L](this, UnsupervisedEstimatorWithData(stage, data))
  }

  /**
   * Adds a new supervised estimator with input data to the end of the pipeline.
   *
   * @param stage New supervised estimator.
   * @param data Data to train the supervised estimator.
   * @param labels Labels to use when training the supervised estimator.
   * @tparam D Output type of the new supervised estimator.
   * @return A new pipeline with the estimator appended.
   */
  def addStage[D : ClassTag](
      stage: SupervisedEstimator[C, D, L],
      data: RDD[C],
      labels: RDD[L]): Pipeline[A, C, D, L] = {
    new Pipeline[A, C, D, L](this, SupervisedEstimatorWithData(stage, data, labels))
  }

  /**
   * Fit the current pipeline on input data.
   *
   * @param in Input data.
   * @param labels Input labels.
   * @return A transformer which can be applied to new data.
   */
  override def fit(in: RDD[A], labels: RDD[L]): Transformer[A, C] = {
    val firstTransformer = initStage match {
      case estimator: UnsupervisedEstimator[A, B] => estimator.fit(in)
      case estimator: SupervisedEstimator[A, B, L] => estimator.fit(in, labels)
      case SupervisedEstimatorWithData(x, data, l) => x.fit(data, l)
      case UnsupervisedEstimatorWithData(x, data) => x.fit(data)
      case transformer: Transformer[A, B] => transformer
      case _ => throw new IllegalArgumentException
    }

    val secondTransformer = nextStage match {
      case estimator: UnsupervisedEstimator[B, C] => estimator.fit(firstTransformer.transform(in))
      case estimator: SupervisedEstimator[B, C, L] => estimator.fit(firstTransformer.transform(in), labels)
      case SupervisedEstimatorWithData(x, data, l) => x.fit(data, l)
      case UnsupervisedEstimatorWithData(x, data) => x.fit(data)
      case transformer: Transformer[B, C] => transformer
      case _ => throw new IllegalArgumentException
    }

    new TransformerChain[A, B, C](firstTransformer, secondTransformer)
  }
}

/**
 * Companion object with useful constructors.
 */
object Pipeline {
  /**
   * Construct a simple two-stage pipeline.
   * @param a First stage.
   * @param b Second stage.
   * @return A pipeline.
   */
  def apply[A : ClassTag, B : ClassTag, C : ClassTag, L](
      a: PipelineStage[A, B],
      b: PipelineStage[B, C]) = new Pipeline[A, B, C, L](a, b)

  /**
   * Construct a one-stage supervised pipeline.
   * @param a Pipeline stage.
   * @tparam A Input type.
   * @tparam B Output type.
   * @tparam L Label type.
   * @return A one-stage supervised pipeline.
   */
  def apply[A : ClassTag, B : ClassTag, L](a: PipelineStage[A, B]) = new Pipeline[A, A, B, L](null, a)

  /**
   * Construct an empty pipeline.
   * @tparam A
   * @tparam L
   * @return An empty pipeline.
   */
  def apply[A : ClassTag, L]() = new Pipeline[A, A, A, L](new IdentityTransformer[A], new IdentityTransformer[A])
}

/**
 * Creates a chain of transformers. (This is only used internally).
 * @param a First transformer.
 * @param b Second transformer.
 * @tparam A Transformer chain input type.
 * @tparam B Intermediate type.
 * @tparam C Output type.
 */
private class TransformerChain[A : ClassTag, B, C : ClassTag](
    val a: Transformer[A, B],
    val b: Transformer[B, C]) extends Transformer[A, C] {

  /**
   * Transform an input.
   * @param in Input.
   * @return Transformed input.
   */
  override def transform(in: A): C = b.transform(a.transform(in))

  /**
   * Transform an RDD of Inputs.
   * @param in Input RDD.
   * @return RDD of transformed inputs.
   */
  override def transform(in: RDD[A]): RDD[C] = b.transform(a.transform(in))
}

/**
 * Special kind of transformer that does nothing.
 */
class IdentityTransformer[T : ClassTag] extends Transformer[T, T] {
  override def transform(in: T): T = in
  override def transform(in: RDD[T]): RDD[T] = in
}

/**
 * A transformer is a deterministic object that takes data of type A and turns it into type B.
 * It differs from a function in that it can operate on RDDs.
 *
 * @tparam A Input type.
 * @tparam B Output type.
 */
abstract class Transformer[A : ClassTag, B : ClassTag]
    extends PipelineNode[RDD[A], RDD[B]]
    with Serializable
    with PipelineStage[A, B] {

  /**
   * Takes an A and returns a B.
   *
   * @param in Input.
   * @return Output.
   */
  def transform(in: A): B

  /**
   * Takes an RDD[A] and returns an RDD[B].
   *
   * @param in Collection of A's.
   * @return Collection of B's.
   */
  def transform(in: RDD[A]): RDD[B] = in.map(transform)

  /**
   * Syntactic sugar for transform().
   *
   * @param in Collection of A's.
   * @return Collection of B's.
   */
  def apply(in: RDD[A]): RDD[B] = transform(in)

  /**
   * Append a transformer onto another transformer, forming a transformer chain.
   * This is the logical equivalent of function composition.
   *
   * @param next Second transformer.
   * @tparam C Output type of second transformer.
   * @return A new transformer chain.
   */
  def andThen[C : ClassTag](next: Transformer[B, C]): Transformer[A, C] = new TransformerChain[A, B, C](this, next)
}

/**
 * An Unsupervised estimator. An estimator can emit a transformer via a call to its fit method on unlabeled data.
 *
 * @tparam I Input type.
 * @tparam O Output type.
 */
abstract class UnsupervisedEstimator[I, O] extends Serializable with PipelineStage[I, O] {
  def fit(in: RDD[I]): Transformer[I, O]
}

/**
 * A Supervised estimator. A supervised estimator emits a transformer via a call to its fit method on labeled data.
 *
 * @tparam I Input type.
 * @tparam O Output type.
 * @tparam L Label type.
 */
abstract class SupervisedEstimator[I, O, L] extends Serializable with PipelineStage[I, O] {
  def fit(data: RDD[I], labels: RDD[L]): Transformer[I, O]
}

/**
 * An unsupervised estimator that carries its data with it.
 * @param e Estimator.
 * @param data Data.
 * @tparam A Input type.
 * @tparam B Output type.
 */
private case class UnsupervisedEstimatorWithData[A, B](
    e: UnsupervisedEstimator[A, B],
    data: RDD[A]) extends PipelineStage[A, B]

/**
 * A supervised estimator that carries its data with it.
 * @param e Estimator.
 * @param data Data.
 * @param labels Labels.
 * @tparam I Input type.
 * @tparam O Output type.
 * @tparam L Label type.
 */
private case class SupervisedEstimatorWithData[I, O, L](
    e: SupervisedEstimator[I, O, L],
    data: RDD[I],
    labels: RDD[L]) extends PipelineStage[I, O]