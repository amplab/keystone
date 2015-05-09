package evaluation

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object MeanAveragePrecisionEvaluator {

  /**
   * Compute the mean average precision for multi-class classification
   * NOTE: The current implementation is only suitable when we have a 
   *       small number of classes and data items.
   * @param testActual - For every test image, this contains list of valid labels. 
   *                     Labels are assumed to be class ids.
   * @param testPredicted - For every test image, this contains a list of scores for each class
   * @param numClasses - Total number of classes
   * @return An array containing average precision scores for each class
   */
  def apply(
      testActual: RDD[Array[Int]],
      testPredicted: RDD[Array[Double]],
      numClasses: Int)
    : Array[Double] = {

    // TODO(shivaram): This might not work well on on large amounts of classes or data.
    // as we group every data item to every class. Thus entire data must fit in a single
    // machine in the current implementation.
    val mapData = testPredicted.zip(testActual).flatMap({ case (scores, actual) =>
      val actualSet = actual.toSet
      (0 until numClasses).map { cl =>
        val l = if(actualSet.contains(cl)) {
          1.0
        } else {
          0.0
        }
        (cl, (l, scores(cl)))
      }
    }).groupByKey(numClasses).map { case (key, vals) =>

      // Sort the scores for this class
      val sortidx = vals.map(_._2).zipWithIndex.toSeq.sortBy(_._1).reverse.map(_._2)
      val gt = vals.map(_._1).toArray

      // For each class, compute the true positive, false positive
      val tps_gt = sortidx.map(x => gt(x))
      val tps = tps_gt.scanLeft(0.0)(_ + _).drop(1)
      val fps_gt = sortidx.map(x => 1.0 - gt(x))
      val fps = fps_gt.scanLeft(0.0)(_ + _).drop(1)

      val total = vals.map(_._1).sum

      // Compute Recall and Precision
      val recalls = tps.map(x => x.toDouble / total).toArray
      val precisions = tps.zip(fps).map(y => y._1 / (y._1 + y._2)).toArray

      (key, getAP(precisions, recalls))
    }

    mapData.collect().sortBy(_._1).map(_._2).toArray
  }

  /**
   * Get average precision at 10 levels given precisions and recalls
   */
  private def getAP(precisions: Array[Double], recalls: Array[Double]) = {
    var ap = 0.0
    val levels = (0 to 10 by 1).map(x => x / 10.0)
    levels.foreach { t =>
      // Find where recalls are greater than t and precision values at those indices
      val px = recalls.toSeq.zipWithIndex.filter(x => x._1 >= t).map(x => precisions(x._2))
      val p = if (px.isEmpty) {
        0.0
      } else {
        px.max
      }
      ap = ap + p / 11.0
    }
    ap
  }

}
