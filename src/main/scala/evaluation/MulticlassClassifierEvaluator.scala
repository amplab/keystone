package evaluation

import breeze.linalg.{*, sum, DenseMatrix}
import org.apache.spark.rdd.RDD

/**
 * TODO: WRITEME
 * @param confusionMatrix Confusion Matrix for the classifier, where the predicted classes are in columns
 */
case class MulticlassClassifierEvaluation(confusionMatrix: DenseMatrix[Double]) {
  require(confusionMatrix.rows == confusionMatrix.cols, "Confusion matrix must be square")

  private val numClasses = confusionMatrix.rows
  val classEval = {
    val total = sum(confusionMatrix)
    val actualsSums = sum(confusionMatrix(*, ::))
    val predictedSums = sum(confusionMatrix(::, *)).t(::, 0)

    (0 until numClasses).map(clss => {
      val tp = confusionMatrix(clss, clss)
      val fp = predictedSums(clss) - tp
      val tn = total - actualsSums(clss) - fp
      val fn = total - tp - fp - tn
      BinaryClassifierEvaluation(tp, fp, tn, fn)
    }).toArray
  }

  private def classAvg(f: BinaryClassifierEvaluation => Double): Double = classEval.map(f).sum / numClasses
  private def micro(f: BinaryClassifierEvaluation => Double): Double = f(classEval.reduce(_ merge _))

  def accuracy: Double = classAvg(_.accuracy)
  def macroPrecision: Double = classAvg(_.precision)
  def macroRecall: Double = classAvg(_.recall)
  def macroFScore(beta: Double = 1.0): Double = classAvg(_.fScore(beta))
  def microPrecision: Double = micro(_.precision)
  def microRecall: Double = micro(_.recall)
  def microFScore(beta: Double = 1.0): Double = micro(_.fScore(beta))

  /**
   * TODO:  FIX UP DOC
   * Returns pretty-printed confusion matrix string:
   * predicted classes are in columns, true labels in rows
   * Styled after Mahout's pprinting
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
   * Encodes an Int in base 26 (using chars 'a' - 'z')
   * Used to make the column headers in the pretty-printed confusion matrix
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
  def apply(predictions: RDD[Int], actuals: RDD[Int], numClasses: Int): MulticlassClassifierEvaluation = {
    MulticlassClassifierEvaluation(confusionMatrix(predictions, actuals, numClasses))
  }

  /**
   * TODO: Fixmeup
   * Builds a confusion matrix:
   * predicted classes are in columns, true labels in the rows
   * they are ordered by class label ascending,
   * as in "labels"
   */
  def confusionMatrix(predictions: RDD[Int], actuals: RDD[Int], numClasses: Int): DenseMatrix[Double] = {
    def incrementCount(x: DenseMatrix[Double], n: (Int, Int)): DenseMatrix[Double] = {
      val predicted = n._1
      val actual = n._2

      x(actual, predicted) = x(actual, predicted) + 1.0
      x
    }

    predictions.zip(actuals).aggregate(new DenseMatrix[Double](numClasses, numClasses))(incrementCount, _ + _)
  }
}
