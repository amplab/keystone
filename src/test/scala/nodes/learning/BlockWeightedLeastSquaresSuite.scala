package nodes.learning

import breeze.linalg._
import breeze.stats.distributions.Rand

import org.apache.spark.SparkContext
import org.scalatest.FunSuite

import java.io._

import pipelines._
import utils.{Stats, MatrixUtils, TestUtils}

class BlockWeightedLeastSquaresSuite extends FunSuite with Logging with LocalSparkContext {

  test("BlockWeighted solver should match solving each class solved separately") {
    val numPerChunk = 4
    val numChunks = 3
    val numPasses = 40
    val nClasses = 3
    val mixtureWeight = 0.3
    val lambda = 0.1

    val aMat = csvread(new File(TestUtils.getTestResourceFileName("aMat.csv")))
    val bMat = csvread(new File(TestUtils.getTestResourceFileName("bMat.csv")))

    val splitAMat = (0 until numChunks).map { i => 
      new DenseMatrix(aMat.rows, numPerChunk,
        aMat(::, (numPerChunk*i) until (numPerChunk*i + numPerChunk)).toArray)
    }

    sc = new SparkContext("local", "test")

    val aRDD = splitAMat.map { mat =>
      sc.parallelize(MatrixUtils.matrixToRowArray(mat), 3).cache()
    }
    val bRDD = sc.parallelize(MatrixUtils.matrixToRowArray(bMat), 3).cache()

    val wsq = BlockWeightedLeastSquares.trainWithL2(
      aRDD, bRDD, lambda, mixtureWeight, numPasses)

    val finalFullModel = wsq.xs.reduceLeft { (a, b) =>
      DenseMatrix.vertcat(a, b)
    }

    println("wsq " + finalFullModel)
    println("Norm of wsq.x " + norm(finalFullModel.toDenseVector))
    println("wsq b is " + wsq.bOpt.get)
  }

}
