package evaluation

import breeze.linalg.DenseVector
import org.scalatest.FunSuite
import org.apache.spark.SparkContext
import utils.Stats
import workflow.PipelineContext

class MeanAveragePrecisionSuite extends FunSuite with PipelineContext {

  test("random map test") {
    sc = new SparkContext("local", "test")

    // Build some random test data with 4 classes 0,1,2,3
    val actual = List(Array(0, 3), Array(2), Array(1, 2), Array(0))
    val actualRdd = sc.parallelize(actual)

    val predicted = List(
      DenseVector(0.1, -0.05, 0.12, 0.5),
      DenseVector(-0.23, -0.45, 0.23, 0.1),
      DenseVector(-0.34, -0.32, -0.66, 1.52),
      DenseVector(-0.1, -0.2, 0.5, 0.8))

    val predictedRdd = sc.parallelize(predicted)

    val map = new MeanAveragePrecisionEvaluator(4).apply(predictedRdd, actualRdd)

    // Expected values from running this in MATLAB
    val expected = DenseVector(1.0, 0.3333, 0.5, 0.3333)

    assert(Stats.aboutEq(map, expected, 1e-4))
  }
}
