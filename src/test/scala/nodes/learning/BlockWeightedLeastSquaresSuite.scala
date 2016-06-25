package nodes.learning

import org.scalatest.FunSuite

import java.io._

import breeze.linalg._
import breeze.stats.distributions.Rand

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import pipelines._
import utils.{Stats, MatrixUtils, TestUtils}
import workflow.PipelineContext

class BlockWeightedLeastSquaresSuite extends FunSuite with Logging with PipelineContext {

  def computeGradient(
      trainingFeatures: RDD[DenseVector[Double]],
      trainingLabels: RDD[DenseVector[Double]],
      lambda: Double,
      mixtureWeight: Double,
      x: DenseMatrix[Double],
      b: DenseVector[Double]): DenseMatrix[Double] = {

    val nTrain = trainingLabels.count
    val trainingLabelsMat = trainingLabels.mapPartitions(part =>
      MatrixUtils.rowsToMatrixIter(part))
    val trainingFeaturesMat = trainingFeatures.mapPartitions(part =>
      MatrixUtils.rowsToMatrixIter(part))

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
    val aMat = csvread(new File(TestUtils.getTestResourceFileName(aMatFile)))
    val bMat = csvread(new File(TestUtils.getTestResourceFileName(bMatFile)))

    val fullARDD = sc.parallelize(MatrixUtils.matrixToRowArray(aMat), numParts).cache()
    val bRDD = sc.parallelize(MatrixUtils.matrixToRowArray(bMat), numParts).cache()
    (fullARDD, bRDD)
  }

  test("BlockWeighted solver solution should work with empty partitions") {
    val blockSize = 4
    val numIter = 10
    val lambda = 0.1
    val mixtureWeight = 0.3
    val numParts = 3

    sc = new SparkContext("local", "test")

    val (fullARDD, bRDD) = loadMatrixRDDs("aMat.csv", "bMat.csv", numParts, sc)
    // Throw away a partition in both features and labels
    val partitionToEmpty = 1
    val aProcessed = fullARDD.mapPartitionsWithIndex { case (idx, iter) =>
      if (idx == partitionToEmpty) {
        Iterator.empty
      } else {
        iter
      }
    }
    val bProcessed = bRDD.mapPartitionsWithIndex { case (idx, iter) =>
      if (idx == partitionToEmpty) {
        Iterator.empty
      } else {
        iter
      }
    }

    val wsq = new BlockWeightedLeastSquaresEstimator(blockSize, numIter, lambda,
      mixtureWeight).fit(aProcessed, bProcessed)
    // TODO: What can we test here ?
    val finalFullModel = wsq.xs.reduceLeft { (a, b) =>
      DenseMatrix.vertcat(a, b)
    }

    // norm(gradient) should be close to zero
    val gradient = computeGradient(aProcessed, bProcessed, lambda, mixtureWeight, finalFullModel,
      wsq.bOpt.get)

    // TODO(shivaram): Gradient seems to be pretty-high here ?
    // But we dropped an entire class ?
    println("norm of gradient is " + norm(gradient.toDenseVector))
  }

  test("Per-class solver solution should match BlockWeighted solver") {
    val blockSize = 4
    val numIter = 5
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

    val pcs = new PerClassWeightedLeastSquaresEstimator(blockSize, numIter, lambda,
      mixtureWeight).fit(fullARDD, bRDD)
    val finalPcsModel = pcs.xs.reduceLeft { (a, b) =>
      DenseMatrix.vertcat(a, b)
    }

    assert(Stats.aboutEq(norm((finalFullModel - finalPcsModel).toDenseVector), 0, 1e-6))
    assert(Stats.aboutEq(norm(wsq.bOpt.get), norm(pcs.bOpt.get), 1e-6))
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

  test("BlockWeighted solver should work with 1 class only") {
    val blockSize = 4
    val numIter = 10
    val lambda = 0.1
    val mixtureWeight = 0.3
    val numParts = 1

    sc = new SparkContext("local", "test")

    val (fullARDD, bRDD) = loadMatrixRDDs("aMat-1class.csv", "bMat-1class.csv", numParts, sc)

    val wsq = new BlockWeightedLeastSquaresEstimator(blockSize, numIter, lambda,
      mixtureWeight).fit(fullARDD, bRDD)

    val finalFullModel = wsq.xs.reduceLeft { (a, b) =>
      DenseMatrix.vertcat(a, b)
    }
    // Nothing to assert here ?
  }

  test("BlockWeighted solver should work with nFeatures not divisible by blockSize") {
    val blockSize = 5 // nFeatures is 12 here
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
    println("norm of WLS gradient is " + norm(gradient.toDenseVector))
    assert(Stats.aboutEq(norm(gradient.toDenseVector), 0, 1e-1))

    // Also verify the per-class solver
    val pcs = new PerClassWeightedLeastSquaresEstimator(blockSize, numIter, lambda,
      mixtureWeight).fit(fullARDD, bRDD)
    val finalPcsModel = pcs.xs.reduceLeft { (a, b) =>
      DenseMatrix.vertcat(a, b)
    }

    val perClassGradient = computeGradient(fullARDD, bRDD, lambda, mixtureWeight, finalPcsModel,
      pcs.bOpt.get)
    println("norm of PCS gradient is " + norm(perClassGradient.toDenseVector))
    assert(Stats.aboutEq(norm(perClassGradient.toDenseVector), 0, 1e-1))
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

  test("PerClass WeightedLeastSquares should work with empty partitions") {
    val blockSize = 4
    val numIter = 10
    val lambda = 0.1
    val mixtureWeight = 0.3
    val numParts = 3
    val nClasses = 3

    sc = new SparkContext("local", "test")

    val (fullARDD, bRDD) = loadMatrixRDDs("aMat.csv", "bMat.csv", numParts, sc)
    // Throw away a partition in both features and labels
    val partitionToEmpty = 1
    val aProcessed = fullARDD.mapPartitionsWithIndex { case (idx, iter) =>
      if (idx == partitionToEmpty) {
        Iterator.empty
      } else {
        iter
      }
    }
    // Also drop one column of labels corresponding to partition we are dropping
    val bProcessed = bRDD.mapPartitionsWithIndex { case (idx, iter) =>
      if (idx == partitionToEmpty) {
        Iterator.empty
      } else {
        val colsToKeep = (0 until nClasses).toSet - partitionToEmpty
        iter.map(x => x(colsToKeep.toSeq).toDenseVector)
      }
    }

    val pcs = new PerClassWeightedLeastSquaresEstimator(blockSize, numIter, lambda,
      mixtureWeight).fit(aProcessed, bProcessed)
    val finalPcsModel = pcs.xs.reduceLeft { (a, b) =>
      DenseMatrix.vertcat(a, b)
    }

    // norm(gradient) should be close to zero
    val gradient = computeGradient(aProcessed, bProcessed, lambda, mixtureWeight, finalPcsModel,
      pcs.bOpt.get)

    // TODO(shivaram): Gradient seems to be pretty-high here ?
    // But we dropped an entire class ?
    println("norm of PCWLS gradient is " + norm(gradient.toDenseVector))
  }

}
