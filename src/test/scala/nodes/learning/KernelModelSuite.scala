package nodes.learning

import breeze.linalg._

import org.apache.spark.SparkContext
import org.scalatest.FunSuite

import workflow.PipelineContext
import utils.{MatrixUtils, Stats}

class KernelModelSuite extends FunSuite with PipelineContext {

  test("KernelModel XOR test") {
    sc = new SparkContext("local", "test")

    val x = Array(DenseVector(-1.0, -1.0), DenseVector(1.0, 1.0), DenseVector(-1.0, 1.0),DenseVector(1.0, -1.0))
    val xTest = Array(DenseVector(-1.0, -1.0), DenseVector(1.0, 1.0), DenseVector(-1.0, 1.0))
    val y = Array(DenseVector(0.0, 1.0), DenseVector(0.0, 1.0), DenseVector(1.0, 0.0), DenseVector(1.0, 0.0))
    val yTest = Array(DenseVector(0.0, 1.0), DenseVector(0.0, 1.0), DenseVector(1.0, 0.0))

    val xRDD = sc.parallelize(x, 2)
    val yRDD = sc.parallelize(y, 2)
    val xTestRDD = sc.parallelize(xTest, 2)

    val gaussian = new GaussianKernelGenerator(10)
    // Set block size to number of data points so no blocking happens
    val clf = new KernelRidgeRegression(gaussian, 0, 4, 2)

    val kernelModel = clf.fit(xRDD, yRDD)
    val yHat = kernelModel(xTestRDD).collect()
    // Fit should be good
    val delta = MatrixUtils.rowsToMatrix(yHat) - MatrixUtils.rowsToMatrix(yTest)

    delta :*= delta
    println("SUM OF DELTA1 " + sum(delta))
    assert(Stats.aboutEq(sum(delta), 0, 1e-4))
  }

  test("KernelModel XOR blocked test") {
    sc = new SparkContext("local", "test")

    val x = Array(DenseVector(-1.0, -1.0), DenseVector(1.0, 1.0), DenseVector(-1.0, 1.0),DenseVector(1.0, -1.0))
    val xTest = Array(DenseVector(-1.0, -1.0), DenseVector(1.0, 1.0), DenseVector(-1.0, 1.0))
    val y = Array(DenseVector(0.0, 1.0), DenseVector(0.0, 1.0), DenseVector(1.0, 0.0), DenseVector(1.0, 0.0))
    val yTest = Array(DenseVector(0.0, 1.0), DenseVector(0.0, 1.0), DenseVector(1.0, 0.0))

    val xRDD = sc.parallelize(x, 2)
    val yRDD = sc.parallelize(y, 2)
    val xTestRDD = sc.parallelize(xTest, 2)

    val gaussian = new GaussianKernelGenerator(10)

    // Set block size to half number of data points so blocking happens
    val clf = new KernelRidgeRegression(gaussian, 0, 2, 2)

    val kernelModel = clf.fit(xRDD, yRDD)
    val yHat = kernelModel(xTestRDD).collect()
    // Fit should be good
    val delta = MatrixUtils.rowsToMatrix(yHat) - MatrixUtils.rowsToMatrix(yTest)

    delta :*= delta
    assert(Stats.aboutEq(sum(delta), 0, 1e-4))
  }
}
