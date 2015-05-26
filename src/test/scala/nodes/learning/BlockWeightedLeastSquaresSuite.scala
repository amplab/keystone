package nodes.learning

import org.scalatest.FunSuite

import java.io._

import breeze.linalg._
import breeze.stats.distributions.Rand

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import pipelines._
import utils.{Stats, MatrixUtils, TestUtils}

class BlockWeightedLeastSquaresSuite extends FunSuite with Logging with LocalSparkContext {

  def computeGradient(
      trainingFeatures: RDD[DenseVector[Double]],
      trainingLabels: RDD[DenseVector[Double]],
      lambda: Double,
      mixtureWeight: Double,
      x: DenseMatrix[Double],
      b: DenseVector[Double]): DenseMatrix[Double] = {

    val nTrain = trainingLabels.count
    val trainingLabelsMat = trainingLabels.mapPartitions(part =>
      Iterator.single(MatrixUtils.rowsToMatrix(part)))
    val trainingFeaturesMat = trainingFeatures.mapPartitions(part =>
      Iterator.single(MatrixUtils.rowsToMatrix(part)))

    val weights = trainingLabelsMat.map { mat =>
      val numPosEx = mat.rows
      val firstLabel = mat(0, ::).t.toArray
      val classIdx = firstLabel.indexOf(firstLabel.max)
      val negWt = (1.0 - mixtureWeight) / nTrain.toDouble
      val posWt = negWt + (mixtureWeight / numPosEx.toDouble)
      val out = DenseMatrix.fill(mat.rows, mat.cols)(negWt)
      out(::, classIdx) := posWt
      out
    }

    val modelBroadcast = trainingFeatures.context.broadcast(x)
    val bBroadcast = trainingFeatures.context.broadcast(b)

    // Compute the gradient!
    val matOut = trainingFeaturesMat.zip(trainingLabelsMat.zip(weights)).map { part =>
      val feats = part._1
      val labels = part._2._1
      val wts = part._2._2
      val out = feats * modelBroadcast.value
      out(*, ::) :+= bBroadcast.value
      out -= labels
      out :*= wts
      feats.t * out
    }.reduce((a: DenseMatrix[Double], b: DenseMatrix[Double]) => a += b)

    val gradW = matOut + modelBroadcast.value * lambda
    gradW
  }

  def loadMatrixRDDs(aMatFile: String, bMatFile: String, numParts: Int, sc: SparkContext) = {
    val aMat = csvread(new File(TestUtils.getTestResourceFileName("aMat.csv")))
    val bMat = csvread(new File(TestUtils.getTestResourceFileName("bMat.csv")))

    val fullARDD = sc.parallelize(MatrixUtils.matrixToRowArray(aMat), numParts).cache()
    val bRDD = sc.parallelize(MatrixUtils.matrixToRowArray(bMat), numParts).cache()
    (fullARDD, bRDD)
  }

  test("BlockWeighted solver solution should have zero gradient") {
    val blockSize = 4
    val numIter = 10
    val lambda = 0.1
    val mixtureWeight = 0.3
    val numParts = 3

    sc = new SparkContext("local", "test")

    val (fullARDD, bRDD) = loadMatrixRDDs("aMat.csv", "bMat.csv", numParts, sc)

    val wsq = new BlockWeightedLeastSquaresEstimator(blockSize, numIter, lambda,
      mixtureWeight).fit(fullARDD, bRDD)

    val finalFullModel = wsq.xs.reduceLeft { (a, b) =>
      DenseMatrix.vertcat(a, b)
    }

    // norm(gradient) should be close to zero
    val gradient = computeGradient(fullARDD, bRDD, lambda, mixtureWeight, finalFullModel,
      wsq.bOpt.get)

    println("norm of gradient is " + norm(gradient.toDenseVector))
    assert(Stats.aboutEq(norm(gradient.toDenseVector), 0, 1e-2))
  }

  test("groupByClasses should work correctly") {
    val lambda = 0.1
    val mixtureWeight = 0.3
    val blockSize = 4
    val numIter = 10
    val numParts = 3

    sc = new SparkContext("local", "test")

    val (fullARDD, bRDD) = loadMatrixRDDs("aMat.csv", "bMat.csv", numParts, sc)

    // To call computeGradient we again the rows grouped correctly
    val (shuffledA, shuffledB) = BlockWeightedLeastSquaresEstimator.groupByClasses(
      Seq(fullARDD), bRDD)

    val wsq = new BlockWeightedLeastSquaresEstimator(blockSize, numIter, lambda,
      mixtureWeight).fit(fullARDD, bRDD)

    val finalFullModel = wsq.xs.reduceLeft { (a, b) =>
      DenseMatrix.vertcat(a, b)
    }

    // norm(gradient) should be close to zero
    val gradient = computeGradient(shuffledA.head, shuffledB, lambda, mixtureWeight, finalFullModel,
      wsq.bOpt.get)

    println("norm of gradient is " + norm(gradient.toDenseVector))
    assert(Stats.aboutEq(norm(gradient.toDenseVector), 0, 1e-2))
  }

}
