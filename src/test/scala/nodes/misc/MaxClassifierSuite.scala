package nodes.misc

import breeze.linalg.DenseVector
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines.LocalSparkContext

class MaxClassifierSuite extends FunSuite with LocalSparkContext {
  test("max classifier") {
    sc = new SparkContext("local", "test")

    assert(MaxClassifier.apply(DenseVector(-10.0, 42.4, 335.23, -43.0)) === 2)
    assert(MaxClassifier.apply(DenseVector(Double.MinValue)) === 0)
    assert(MaxClassifier.apply(DenseVector(3.0, -23.2, 2.99)) === 0)
  }
}
