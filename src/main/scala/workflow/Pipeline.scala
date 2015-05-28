package workflow

import scala.reflect.ClassTag

/**
 * Created by tomerk11 on 5/28/15.
 */
case class Pipeline[A, B : ClassTag] private[workflow] (nodes: Seq[Node[_, _]]) extends Node[A, B] {
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
