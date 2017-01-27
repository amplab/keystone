package utils

import org.scalatest.FunSuite

import breeze.linalg._
import breeze.stats._

import org.apache.spark.SparkContext

import pipelines._
import workflow.PipelineContext

class MatrixUtilsSuite extends FunSuite with PipelineContext {

  test("computeMean works correctly") {
    val numRows = 1000
    val numCols = 32
    val numParts = 4
    sc = new SparkContext("local", "test")
    val in = DenseMatrix.rand(numRows, numCols)
    val inArr = MatrixUtils.matrixToRowArray(in)
    val rdd = sc.parallelize(inArr, numParts).mapPartitions { iter => 
      Iterator.single(MatrixUtils.rowsToMatrix(iter))
    }
    val expected = mean(in(::, *)).t
    val actual = MatrixUtils.computeMean(rdd)
    assert(Stats.aboutEq(expected, actual, 1e-6))
  }

}
