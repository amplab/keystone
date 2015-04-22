package utils

import pipelines._

import org.scalatest.FunSuite

import breeze.linalg._
import breeze.numerics._
import breeze.stats._

class RandomMatrixSuite extends FunSuite {

test("randMatrixUniform") {
  val m:DenseMatrix[Double] = Stats.randMatrixUniform(100,100);
  assert(max(m) <= 1) 
  assert(min(m) >= 0)
  assert(m.rows == 100) 
  assert(m.cols == 100)
}

test("randMatrixGaussian"){
  val m:DenseMatrix[Double] = Stats.randMatrixGaussian(1000,1000);
  // What other tests would make sense here?
  assert(m.rows == 1000) 
  assert(m.cols == 1000)
  assert(mean(m) < 10e-2)
  assert(variance(m) - 1 < 10e-3)
}

test("randMatrixCauchy"){
  val m:DenseMatrix[Double] = Stats.randMatrixCauchy(1000,1000);
  // What other tests would make sense here?
  assert(m.rows == 1000) 
  assert(m.cols == 1000)
  assert(median(m) < 10e-3)
}

}
