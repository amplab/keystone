package evaluation

import org.apache.spark.rdd.RDD

case class BinaryClassifierEvaluation(tp: Double, fp: Double, tn: Double, fn: Double) {
  def merge(other: BinaryClassifierEvaluation) = {
    BinaryClassifierEvaluation(tp + other.tp, fp + other.fp, tn + other.tn, fn + other.fn)
  }

  def accuracy: Double = (tp + tn) / (tp + fp + tn + fn)
  def recall: Double = tp / (tp + fn)
  def precision: Double = tp / (tp + fp)
  def specificity: Double = tn / (fp + tn)
  def auc: Double = 0.5 * ((tp / (tp + fn)) + (tn / (tn + fp)))

  def fScore(beta: Double = 1.0): Double = {
    val num = (1.0 + beta * beta) * tp
    val denom = (1.0 + beta * beta) * tp + beta * beta * fn + fp
    num / denom
  }
}

object BinaryClassifierEvaluator {
  def apply(predictions: RDD[Boolean], actuals: RDD[Boolean]): BinaryClassifierEvaluation = {
    predictions.zip(actuals).map { case (pred, actual) =>
      val tp = if (pred && actual) 1d else 0d
      val fp = if (pred && !actual) 1d else 0d
      val tn = if (!pred && !actual) 1d else 0d
      val fn = if (!pred && actual) 1d else 0d
      BinaryClassifierEvaluation(tp, fp, tn, fn)
    }.reduce(_ merge _)
  }
}
