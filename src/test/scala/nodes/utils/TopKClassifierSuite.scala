package nodes.utils

import breeze.linalg.DenseVector
import nodes.util.TopKClassifier
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines.LocalSparkContext

class TopKClassifierSuite extends FunSuite with LocalSparkContext {
  test("top k classifier, k <= vector size") {
    sc = new SparkContext("local", "test")

    assert(TopKClassifier(2).apply(DenseVector(-10.0, 42.4, -43.0, 23.0)) === Array(1, 3))
    assert(TopKClassifier(4).apply(DenseVector(Double.MinValue, Double.MaxValue, 12.0, 11.0, 10.0)) === Array(1, 2, 3, 4))
    assert(TopKClassifier(3).apply(DenseVector(3.0, -23.2, 2.99)) === Array(0, 2, 1))
  }

  test("top k classifier, k > vector size") {
    sc = new SparkContext("local", "test")

    assert(TopKClassifier(5).apply(DenseVector(-10.0, 42.4, -43.0, 23.0)) === Array(1, 3, 0, 2))
    assert(TopKClassifier(2).apply(DenseVector(Double.MinValue)) === Array(0))
    assert(TopKClassifier(20).apply(DenseVector(3.0, -23.2, 2.99)) === Array(0, 2, 1))
  }

}
