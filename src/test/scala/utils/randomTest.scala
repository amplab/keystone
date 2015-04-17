package utils

import pipelines._

import org.scalatest.FlatSpec

import breeze.linalg._
import breeze.numerics._
import breeze.stats._

class RandSuite extends FlatSpec {

"randMatrixUniform" should "be a random matrix where all values are sampled from U(0,1)" in {
  val m:DenseMatrix[DataType] = Stats.randMatrixUniform(100,100);
  assert(max(m) <= 1) 
  assert(min(m) >= 0)
  assert(m.rows == 100) 
  assert(m.cols == 100)
}

"randMatrixGaussian" should "be a random matrix where all values are sampled from N(0,1)" in {
  val m:DenseMatrix[DataType] = Stats.randMatrixUniform(100,100);
  // What other tests would make sense here?
  assert(m.rows == 100) 
  assert(m.cols == 100)
}

"randMatrixCauchy" should "be a random matrix where all values are sampled from Cauchy(0,1)" in {
  val m:DenseMatrix[DataType] = Stats.randMatrixUniform(100,100);
  // What other tests would make sense here?
  assert(m.rows == 100) 
  assert(m.cols == 100)
}

}
