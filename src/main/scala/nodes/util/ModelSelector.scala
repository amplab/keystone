package nodes.util

import org.apache.spark.rdd.RDD
import pipelines.Logging
import workflow.{WeightedNode, Transformer, LabelEstimator}

import scala.reflect.ClassTag

/**
 * This estimator is used to select whichever input branch has a better validation on training data.
 * It can be used for a naive Hyperparameter grid search.
 *
 * Note: This is very inefficient, because it has to compute all the input once for each path.
 *
 * @param numChoices The number of paths
 * @param evaluator The validation metric to use
 * @tparam T The output type of each branch if this follows a gather node.
 * @tparam L The type of label this node expects
 */
case class ModelSelector[T : ClassTag, L](numChoices: Int, evaluator: (RDD[T], RDD[L]) => Double )
  extends LabelEstimator[Seq[T], T, L] with WeightedNode with Logging {
  override protected def fit(data: RDD[Seq[T]], labels: RDD[L]): Transformer[Seq[T], T] = {
    val choices = for (i <- 0 until numChoices) yield {
      val selector = ModelSelectorDecision[T](i)
      val evaluation = evaluator(selector(data), labels)
      logInfo(s"Evaluation for option $i is $evaluation")
      (selector, evaluation)
    }

    val decision = choices.maxBy(_._2)
    logInfo(s"Chose option ${decision._1.index} with evaluation ${decision._2}")
    decision._1
  }

  override val weight: Int = numChoices
}

case class ModelSelectorDecision[T : ClassTag](index: Int) extends Transformer[Seq[T], T] {
  override def apply(in: Seq[T]): T = in(index)
}
