package utils

import breeze.linalg.{DenseVector, SparseVector, DenseMatrix}
import org.scalatest.FunSuite
import pipelines.Logging

class MatrixMultiplySuite extends FunSuite with Logging {
  test("Vector Matrix mult bench") {
    // Initialize
    {
      val mat = DenseMatrix.rand[Double](5000, 5000)
      val secondMat = DenseMatrix.rand[Double](5000, 5000)
      val denseStart = System.nanoTime()
      val outMat = mat * secondMat

    }

    for (
      n <- Seq(100, 1000, 5000);//, 10000);
      d <- Seq(100, 1000, 5000);//, 10000);
      k <- Seq(1, 5, 10, 100)
    ) {
      var denseTime: Long = 0
      var denseMatTime: Long = 0
      var sparseTime: Long = 0

      val numTrials = 3
      for (i <- 0 until numTrials) {

        val weights = DenseMatrix.rand[Double](d, k)
        val vecs = Seq.fill(n)(DenseVector.rand[Double](d))
        val denseVecs = MatrixUtils.rowsToMatrix(vecs)
        val sparseVecs = vecs.map(x => SparseVector(x.data))

        /*val denseStart = System.nanoTime()
        vecs.foreach { denseVec =>
          weights.t * denseVec
        }
        val denseEnd = System.nanoTime()

        denseTime += (denseEnd - denseStart)

        val denseMatStart = System.nanoTime()
        denseVecs * weights
        val denseMatEnd = System.nanoTime()

        denseMatTime += (denseMatEnd - denseMatStart) */

        val sparseStart = System.nanoTime()
        sparseVecs.foreach { sparseVec =>
          weights.t * sparseVec
        }
        val sparseEnd = System.nanoTime()
        sparseTime += (sparseEnd - sparseStart)
      }

      //logInfo(s"Dense vec multiply time for n: $n d: $d k: $k is $denseTime")
      //logInfo(s"Dense mat multiply time for n: $n d: $d k: $k is $denseMatTime")
      logInfo(s"Sparse vec multiply time for n: $n d: $d k: $k is $sparseTime")
      //logInfo(s"Sparse vec overhead for n: $n d: $d k: $k is ${sparseTime.toDouble / denseTime.toDouble}")

    }
  }
}