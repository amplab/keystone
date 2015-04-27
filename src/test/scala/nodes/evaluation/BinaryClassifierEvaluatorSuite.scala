package nodes.evaluation

import evaluation.BinaryClassifierEvaluator
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines.LocalSparkContext
import utils.Stats

class BinaryClassifierEvaluatorSuite extends FunSuite with LocalSparkContext {
  test("Multiclass evaluation metrics") {
    /*
     * Contingency table for binary classification with total 12 instances:
     * |6|2| true label: positive
     * |1|3| true label: negative
     */
    sc = new SparkContext("local", "test")

    val predictionAndLabels = sc.parallelize( Seq.fill(6)((true, true)) ++ Seq.fill(2)((false, true))
        ++ Seq.fill(1)((true, false)) ++ Seq.fill(3)((false, false)), 2)
    val metrics = BinaryClassifierEvaluator(predictionAndLabels.map(_._1), predictionAndLabels.map(_._2))

    assert(metrics.tp === 6)
    assert(metrics.fp === 1)
    assert(metrics.tn === 3)
    assert(metrics.fn === 2)

    assert(Stats.aboutEq(metrics.precision, 6.0/7.0))
    assert(Stats.aboutEq(metrics.recall, 6.0/8.0))
    assert(Stats.aboutEq(metrics.accuracy, 9.0/12.0))
    assert(Stats.aboutEq(metrics.specificity, 3.0/4.0))
    assert(Stats.aboutEq(metrics.fScore(), 2.0 * 6.0 / (2.0 * 6.0 + 2.0 + 1.0)))
  }
}
