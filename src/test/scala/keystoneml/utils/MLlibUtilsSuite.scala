package keystoneml.utils

import org.apache.spark.mllib.linalg._
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV}
import org.scalatest.FunSuite

class MLlibUtilsSuite extends FunSuite {
  val arr = Array(0.1, 0.2, 0.3, 0.4)
  val n = 20
  val indices = Array(0, 3, 5, 10, 13)
  val values = Array(0.1, 0.5, 0.3, -0.8, -1.0)

  test("dense vector to breeze dense") {
    val vec = Vectors.dense(arr)
    assert(MLlibUtils.mllibVectorToDenseBreeze(vec) === new BDV[Double](arr))
  }

  test("sparse vector to breeze dense") {
    val vec = Vectors.sparse(n, indices, values)
    val breeze = new BDV[Double](n)
    indices.zip(values).foreach { case (x, y) =>
      breeze(x) = y
    }
    assert(MLlibUtils.mllibVectorToDenseBreeze(vec) === breeze)
  }

  test("dense breeze to vector") {
    val breeze = new BDV[Double](arr)
    val vec = MLlibUtils.breezeVectorToMLlib(breeze).asInstanceOf[DenseVector]
    assert(vec.size === arr.length)
    assert(vec.values.eq(arr), "should not copy data")
  }

  test("sparse breeze to vector") {
    val breeze = new BSV[Double](indices, values, n)
    val vec = MLlibUtils.breezeVectorToMLlib(breeze).asInstanceOf[SparseVector]
    assert(vec.size === n)
    assert(vec.indices.eq(indices), "should not copy data")
    assert(vec.values.eq(values), "should not copy data")
  }

  test("sparse breeze with partially-used arrays to vector") {
    val activeSize = 3
    val breeze = new BSV[Double](indices, values, activeSize, n)
    val vec = MLlibUtils.breezeVectorToMLlib(breeze).asInstanceOf[SparseVector]
    assert(vec.size === n)
    assert(vec.indices === indices.slice(0, activeSize))
    assert(vec.values === values.slice(0, activeSize))
  }

  test("dense matrix to breeze dense") {
    val mat = Matrices.dense(3, 2, Array(0.0, 1.0, 2.0, 3.0, 4.0, 5.0))
    val breeze = MLlibUtils.mllibMatrixToDenseBreeze(mat)
    assert(breeze.rows === mat.numRows)
    assert(breeze.cols === mat.numCols)
    assert(breeze.data.eq(mat.asInstanceOf[DenseMatrix].values), "should not copy data")
  }

  test("sparse matrix to breeze dense") {
    val values = Array(1.0, 2.0, 4.0, 5.0)
    val colPtrs = Array(0, 2, 4)
    val rowIndices = Array(1, 2, 1, 2)
    val mat = Matrices.sparse(3, 2, colPtrs, rowIndices, values)
    val breeze = MLlibUtils.mllibMatrixToDenseBreeze(mat)
    assert(breeze.rows === mat.numRows)
    assert(breeze.cols === mat.numCols)
    assert(breeze.toArray === mat.toArray)
  }
}
