package evaluation

import breeze.linalg.{*, sum, DenseMatrix}
import org.apache.spark.rdd.RDD
import pipelines.text.NewsgroupsPipeline._

/**
 * Contains the confusion matrix for a multiclass classifier,
 * and provides common metrics such as micro & macro precision & recall
 *
 * Similar to MLlib's [[org.apache.spark.mllib.evaluation.MulticlassMetrics]],
 * but only does one pass over the data to calculate everything
 *
 * Sample metrics compared at:
 * Sokolova, M., & Lapalme, G. (2009). A systematic analysis of performance measures for
 * classification tasks. Information Processing and Management, 45, p. 427-437
 * http://rali.iro.umontreal.ca/rali/sites/default/files/publis/SokolovaLapalme-JIPM09.pdf
 *
 * @param confusionMatrix  rows are the true labels, cols are the predicted labels
 */
case class MulticlassMetrics(confusionMatrix: DenseMatrix[Double]) {
  require(confusionMatrix.rows == confusionMatrix.cols, "Confusion matrix must be square")

  private val numClasses = confusionMatrix.rows
  val classMetrics = {
    val total = sum(confusionMatrix)
    val actualsSums = sum(confusionMatrix(*, ::))
    val predictedSums = sum(confusionMatrix(::, *)).t(::, 0)

    (0 until numClasses).map(clss => {
      val tp = confusionMatrix(clss, clss)
      val fp = predictedSums(clss) - tp
      val tn = total - actualsSums(clss) - fp
      val fn = total - tp - fp - tn
      BinaryClassificationMetrics(tp, fp, tn, fn)
    }).toArray
  }

  private def classAvg(f: BinaryClassificationMetrics => Double): Double = classMetrics.map(f).sum / numClasses
  private def micro(f: BinaryClassificationMetrics => Double): Double = f(classMetrics.reduce(_ merge _))

  def avgAccuracy: Double = classAvg(_.accuracy)
  def avgError: Double = classAvg(_.error)
  def macroPrecision: Double = classAvg(_.precision)
  def macroRecall: Double = classAvg(_.recall)
  def macroFScore(beta: Double = 1.0): Double = classAvg(_.fScore(beta))
  def totalAccuracy: Double = micro(_.precision)
  def totalError: Double = micro(x => x.fn / (x.fn + x.tp))
  def microPrecision: Double = micro(_.precision)
  def microRecall: Double = micro(_.recall)
  def microFScore(beta: Double = 1.0): Double = micro(_.fScore(beta))


  /**
   * Pretty-prints the confusion matrix in a style similar to Mahout.
   * Predicted labels are in columns. True labels in rows
   *
   * @param classes  An array containing the class names, where the indices are the class labels
   * @return  the pretty-printed string
   */
  def pprintConfusionMatrix(classes: Array[String]): String = {
    val out = new DenseMatrix[Any](numClasses + 3, numClasses + 1)
    out(0, numClasses) = "<-- Classified As"
    (0 until numClasses).foreach(row => {
      out(row + 2, numClasses) = getSmallLabel(row) + " = " + classes(row)
    })

    (0 until numClasses).foreach(col => {
      out(0, col) = getSmallLabel(col)
    })
    (0 until numClasses).foreach(row => {
      (0 until numClasses).foreach(col => {
        out(row + 2, col) = confusionMatrix(row, col).toInt
      })
    })
    out.toString(Int.MaxValue, Int.MaxValue)
  }

  /**
   * Pretty-prints a summary of how the multiclass classifier did (including the confusion matrix)
   *
   * @param classes  An array containing the class names, where the indices are the class labels
   * @return  the pretty-printed string
   */
  def summary(classes: Array[String]): String = {
    val fmt = "%2.3f"
    s"""${pprintConfusionMatrix(classes)}
       |Avg Accuracy:\t${fmt.format(avgAccuracy)}
       |Macro Precision:\t${fmt.format(macroPrecision)}
       |Macro Recall:\t${fmt.format(macroRecall)}
       |Macro F1:\t${fmt.format(macroFScore())}
       |Total Accuracy:\t${fmt.format(totalAccuracy)}
       |Micro Precision:\t${fmt.format(microPrecision)}
       |Micro Recall:\t${fmt.format(microRecall)}
       |Micro F1:\t${fmt.format(microFScore())}
     """.stripMargin
  }

  /**
   * Encodes an Int in base 26 (using chars 'a' - 'z')
   * Used to make the column headers in the pretty-printed confusion matrix (Styled after Mahout)
   * @param i the Int to encode
   * @return The base 26 encoded Int as a String
   */
  private def getSmallLabel(i: Int): String = {
    if (i == 0) {
      return "a"
    }

    var out = ""
    var value: Int = i
    while (value > 0) {
      val n = value % 26
      out = out + ('a' + n).toChar
      value /= 26
    }
    out
  }
}

object MulticlassClassifierEvaluator {
  /**
   * Builds the confusion matrix for a multiclass classifier,
   * and provides common metrics such as micro & macro precision & recall
   *
   * Similar to MLlib's [[org.apache.spark.mllib.evaluation.MulticlassMetrics]],
   * but only does one pass over the data to calculate everything
   *
   * @param predictions  An RDD of predicted class labels. Must range from 0 until numClasses
   * @param actuals  An RDD of the true class labels. Must range from 0 until numClasses
   * @param numClasses  The number of classes being classified among
   * @return  Common multiclass classifier metrics for this data
   */
  def apply(predictions: RDD[Int], actuals: RDD[Int], numClasses: Int): MulticlassMetrics = {
    MulticlassMetrics(calculateConfusionMatrix(predictions, actuals, numClasses))
  }

  /**
   * Builds a confusion matrix from the predictions & true labels & # of classes.
   * columns represent predicted labels, rows represent true labels
   */
  private def calculateConfusionMatrix(predictions: RDD[Int], actuals: RDD[Int], numClasses: Int): DenseMatrix[Double] = {
    def incrementCount(x: DenseMatrix[Double], n: (Int, Int)): DenseMatrix[Double] = {
      val predicted = n._1
      val actual = n._2

      x(actual, predicted) = x(actual, predicted) + 1.0
      x
    }

    predictions.zip(actuals).aggregate(new DenseMatrix[Double](numClasses, numClasses))(incrementCount, _ + _)
  }
}
