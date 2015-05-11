package nodes.utils

import breeze.linalg.DenseVector
import org.scalatest.FunSuite

class MaxClassifierSuite extends FunSuite {
  test("max classifier") {
    assert(MaxClassifier.apply(DenseVector(-10.0, 42.4, 335.23, -43.0)) === 2)
    assert(MaxClassifier.apply(DenseVector(Double.MinValue)) === 0)
    assert(MaxClassifier.apply(DenseVector(3.0, -23.2, 2.99)) === 0)
  }
}
