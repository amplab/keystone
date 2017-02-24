package evaluation

import org.apache.spark.rdd.RDD

/**
 * Contains the contingency table for a binary classifier,
 * and provides common metrics such as precision & recall
 *
 * Similar to MLlib's [[org.apache.spark.mllib.evaluation.BinaryClassificationMetrics]],
 * but only for metrics that can be calculated from the contingency table
 *
 * @param tp  True positive count
 * @param fp  False positive count
 * @param tn  True negative count
 * @param fn  False negative count
 */
case class BinaryClassificationMetrics(tp: Double, fp: Double, tn: Double, fn: Double) {
  /** Merge this contingency table with another */
  def merge(other: BinaryClassificationMetrics) = {
    BinaryClassificationMetrics(tp + other.tp, fp + other.fp, tn + other.tn, fn + other.fn)
  }

  def accuracy: Double = (tp + tn) / (tp + fp + tn + fn)
  def error: Double = (fp + fn) / (tp + fp + tn + fn)
  def recall: Double = tp / (tp + fn)
  def precision: Double = tp / (tp + fp)
  def specificity: Double = tn / (fp + tn)

  /**
   * Calculate the f_beta-score for the binary classifier
   *
   * "Measures the effectiveness of retrieval with respect to a user
   * who attaches beta times as much importance to recall as precision"
   * http://en.wikipedia.org/wiki/F1_score
   * 
   * @param beta Defaults to 1 (so returns the f1-score)
   */
  def fScore(beta: Double = 1.0): Double = {
    val num = (1.0 + beta * beta) * tp
    val denom = (1.0 + beta * beta) * tp + beta * beta * fn + fp
    num / denom
  }

  /**
   * Pretty-prints a summary of how the multiclass classifier did (including the confusion matrix)
   *
   * @return  the pretty-printed string
   */
  def summary(format: String = "%2.3f"): String = {
    s""" Accuracy:\t${format.format(accuracy)}
          |Precision:\t${format.format(precision)}
          |Recall:\t${format.format(recall)}
          |Specificity:\t${format.format(specificity)}}
          |F1:\t${format.format(fScore())}
     """.stripMargin
  }
}

object BinaryClassifierEvaluator extends Evaluator[Boolean, Boolean, BinaryClassificationMetrics] with Serializable {
  /**
   * Calculates the contingency table and binary classification metrics for a binary classifier
   * given predictions and truth labels
   *
   * Similar to MLlib's [[org.apache.spark.mllib.evaluation.BinaryClassificationMetrics]],
   * but runs in one pass, but only contains metrics that can be calculated from the contingency table
   *
   * @param predictions  An RDD[Boolean] containing the predictions
   * @param actuals  An RDD[Boolean] containing the true labels, must be zippable with predictions
   */
  def apply(predictions: RDD[Boolean], actuals: RDD[Boolean]): BinaryClassificationMetrics = {
    predictions.zip(actuals).map { case (pred, actual) =>
      val tp = if (pred && actual) 1d else 0d
      val fp = if (pred && !actual) 1d else 0d
      val tn = if (!pred && !actual) 1d else 0d
      val fn = if (!pred && actual) 1d else 0d
      BinaryClassificationMetrics(tp, fp, tn, fn)
    }.reduce(_ merge _)
  }
}
