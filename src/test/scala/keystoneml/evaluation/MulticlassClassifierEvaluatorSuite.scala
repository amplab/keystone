package keystoneml.evaluation

import breeze.linalg.DenseMatrix
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import keystoneml.workflow.PipelineContext

class MulticlassClassifierEvaluatorSuite extends FunSuite with PipelineContext {
  test("Multiclass keystoneml.evaluation metrics") {
    /*
     * Confusion matrix for 3-class classification with total 9 instances:
     * |2|1|1| true class0 (4 instances)
     * |1|3|0| true class1 (4 instances)
     * |0|0|1| true class2 (1 instance)
     */
    sc = new SparkContext("local", "test")
    val confusionMatrix = new DenseMatrix(3, 3, Array(2, 1, 0, 1, 3, 0, 1, 0, 1))
    val labels = Array(0.0, 1.0, 2.0)
    val predictionAndLabels = sc.parallelize(
      Seq((0.0, 0.0), (0.0, 1.0), (0.0, 0.0), (1.0, 0.0), (1.0, 1.0),
        (1.0, 1.0), (1.0, 1.0), (2.0, 2.0), (2.0, 0.0)), 2)
    val evaluator = new MulticlassClassifierEvaluator(3)
    val metrics = evaluator.evaluate(predictionAndLabels.map(_._1.toInt), predictionAndLabels.map(_._2.toInt)
    )
    val delta = 0.0000001
    val precision0 = 2.0 / (2 + 1)
    val precision1 = 3.0 / (3 + 1)
    val precision2 = 1.0 / (1 + 1)
    val recall0 = 2.0 / (2 + 2)
    val recall1 = 3.0 / (3 + 1)
    val recall2 = 1.0 / (1 + 0)
    val f1measure0 = 2 * precision0 * recall0 / (precision0 + recall0)
    val f1measure1 = 2 * precision1 * recall1 / (precision1 + recall1)
    val f1measure2 = 2 * precision2 * recall2 / (precision2 + recall2)
    val f2measure0 = (1 + 2 * 2) * precision0 * recall0 / (2 * 2 * precision0 + recall0)
    val f2measure1 = (1 + 2 * 2) * precision1 * recall1 / (2 * 2 * precision1 + recall1)
    val f2measure2 = (1 + 2 * 2) * precision2 * recall2 / (2 * 2 * precision2 + recall2)

    assert(metrics.confusionMatrix.toArray.sameElements(confusionMatrix.toArray))
    assert(math.abs(metrics.classMetrics(0).precision - precision0) < delta)
    assert(math.abs(metrics.classMetrics(1).precision - precision1) < delta)
    assert(math.abs(metrics.classMetrics(2).precision - precision2) < delta)
    assert(math.abs(metrics.classMetrics(0).recall - recall0) < delta)
    assert(math.abs(metrics.classMetrics(1).recall - recall1) < delta)
    assert(math.abs(metrics.classMetrics(2).recall - recall2) < delta)
    assert(math.abs(metrics.classMetrics(0).fScore() - f1measure0) < delta)
    assert(math.abs(metrics.classMetrics(1).fScore() - f1measure1) < delta)
    assert(math.abs(metrics.classMetrics(2).fScore() - f1measure2) < delta)
    assert(math.abs(metrics.classMetrics(0).fScore(2.0) - f2measure0) < delta)
    assert(math.abs(metrics.classMetrics(1).fScore(2.0) - f2measure1) < delta)
    assert(math.abs(metrics.classMetrics(2).fScore(2.0) - f2measure2) < delta)

    assert(math.abs(metrics.microRecall -
        (2.0 + 3.0 + 1.0) / ((2 + 3 + 1) + (1 + 1 + 1))) < delta)
    assert(math.abs(metrics.microRecall - metrics.microPrecision) < delta)
    assert(math.abs(metrics.microRecall - metrics.microFScore()) < delta)
    assert(math.abs(metrics.macroPrecision -
        (precision0 + precision1 + precision2) / 3.0) < delta)
    assert(math.abs(metrics.macroRecall -
        (recall0 + recall1 + recall2) / 3.0) < delta)
    assert(math.abs(metrics.macroFScore() -
        (f1measure0 + f1measure1 + f1measure2) / 3.0) < delta)
    assert(math.abs(metrics.macroFScore(2.0) -
        (f2measure0 + f2measure1 + f2measure2) / 3.0) < delta)
  }
}
