package workflow

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Created by tomerk11 on 5/28/15.
 */
case class Pipeline[A, B : ClassTag] private[workflow] (nodes: Seq[Node[_, _]]) extends Node[A, B] {

  /**
   * Chains an estimator onto this Pipeline, producing a new estimator that when fit on same input type as
   * this Pipeline, chains this pipeline with the transformer output by the original Estimator.
   * @param est The estimator to chain onto the Transformer
   * @return  The output estimator
   */
  def thenEstimator[C : ClassTag](est: Estimator[B, C]): Estimator[A, C] = {
    PipelineWithEstimator(this, est)
  }

  /**
   * Chains a Label Estimator onto this Pipeline, producing a new Label Estimator that when fit on same input
   * type as this Pipeline, chains this pipeline with the transformer output by the original Label Estimator.
   * @param est The label estimator to chain onto the Transformer
   * @return  The output label estimator
   */
  def thenLabelEstimator[C : ClassTag, L : ClassTag](est: LabelEstimator[B, C, L]): LabelEstimator[A, C, L] = {
    PipelineWithLabelEstimator(this, est)
  }

  /**
   * Chains another Node onto this pipeline
   * @param next The Transformer to attach to the end of this pipeline
   * @return A new pipeline made up of this one and the new Transformer
   */
  def then[C : ClassTag](next: Node[B, C]): Pipeline[A, C] = {
    Pipeline(nodes.flatMap(_.rewrite) ++ next.rewrite)
  }

  /**
   * Chains a method as a Transformer onto the end of this Pipeline
   * @param next The method to apply to each item
   * @return The new pipeline
   */
  def thenFunction[C : ClassTag](next: B => C): Pipeline[A, C] = then(Transformer(next))

  /**
   * Fits this pipeline
   * @return a new [[PipelineModel]] that may be applied to new data items
   */
  def fit(): PipelineModel[A, B] = PipelineModel(Pipeline.fit(nodes))

  override def rewrite: Seq[Node[_, _]] = {
    val rewrittenNodes = nodes.flatMap(_.rewrite) match {
      case Pipeline(`nodes`) +: tail => `nodes` ++ tail
      case rewritten => rewritten
    }

    if (rewrittenNodes.forall(_.canSafelyPrependExtraNodes)) {
      rewrittenNodes
    } else {
      Seq(Pipeline(nodes.flatMap(_.rewrite)))
    }
  }

  def canSafelyPrependExtraNodes = true
}

object Pipeline {
  def fit(pipeline: Seq[Node[_, _]], prefix: Seq[Transformer[_, _]] = Seq.empty): Seq[Transformer[_, _]] = {
    var transformers: Seq[Transformer[_, _]] = Seq()
    pipeline.flatMap(_.rewrite).foreach { node =>
      val nextTransformer = node match {
        case EstimatorWithData(est, data) => {
          est.unsafeFit(PipelineModel(prefix ++ transformers).unsafeRDDApply(data))
        }
        case LabelEstimatorWithData(est, data, labels) => {
          est.unsafeFit(PipelineModel(prefix ++ transformers).unsafeRDDApply(data), labels)
        }
        case ScatterNode(branches) => {
          ScatterTransformer(branches.map(branch => fit(branch, transformers)))
        }
        case pipeline: Pipeline[_, _] => pipeline.fit()
        case node: Transformer[_, _] => node
      }
      transformers = transformers ++ nextTransformer.rewrite
    }

    transformers
  }
}
