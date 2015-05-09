package nodes.evaluation

import org.scalatest.FunSuite

import evaluation.MeanAveragePrecisionEvaluator
import pipelines.LocalSparkContext
import org.apache.spark.SparkContext
import utils.Stats

class MeanAveragePrecisionSuite extends FunSuite with LocalSparkContext {

  test("random map test") {
    sc = new SparkContext("local", "test")

    // Build some random test data with 4 classes 0,1,2,3
    val actual = List(Array(0, 3), Array(2), Array(1, 2), Array(0))
    val actualRdd = sc.parallelize(actual)

    val predicted = List(Array(0.1, -0.05, 0.12, 0.5), Array(-0.23, -0.45, 0.23, 0.1),
      Array(-0.34, -0.32, -0.66, 1.52), Array(-0.1, -0.2, 0.5, 0.8))
    val predictedRdd = sc.parallelize(predicted)

    val map = MeanAveragePrecisionEvaluator.apply(actualRdd, predictedRdd, 4)

    // Expected values from running this in MATLAB
    val expected = Array(1.0, 0.3333, 0.5, 0.3333)
    map.zip(expected).foreach { x => 
      assert(Stats.aboutEq(x._1, x._2, 1e-4))
    }
  }
}
