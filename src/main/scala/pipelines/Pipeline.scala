package pipelines

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Helper object contains fundamental type definition. A node is a unary function.
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
