package workflow

import breeze.linalg.DenseVector

import scala.reflect.ClassTag

/**
 * Created by tomerk11 on 5/28/15.
 */
case class Pipeline[A, B : ClassTag] private[workflow] (nodes: Seq[Node[_, _]]) extends Node[A, B] {
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
    Pipeline(nodes.flatMap(_.rewrite) ++ next.rewrite)
  }

  /**
   * Chains a method, producing a new Transformer that applies the method to each
   * output item after applying this Transformer first.
   * @param next The method to apply to each item output by this transformer
   * @return The output Transformer
   */
  def thenFunction[C : ClassTag](next: B => C): Pipeline[A, C] = this.then(Transformer(next))

  def fit(): PipelineModel[A, B] = PipelineModel(fit(nodes, Seq()))

  private def fit(pipeline: Seq[Node[_, _]], prefix: Seq[Transformer[_, _]]): Seq[Transformer[_, _]] = {
    var transformers: Seq[Transformer[_, _]] = Seq()
    nodes.flatMap(_.rewrite).foreach { node =>
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
