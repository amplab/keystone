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

  test("BlockWeighted solver solution should have zero gradient") {
    val numPerChunk = 4
    val numChunks = 3
    val numPasses = 10
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

    val fullARDD = sc.parallelize(MatrixUtils.matrixToRowArray(aMat), 3).cache()

    val aRDDs = splitAMat.map { mat =>
      sc.parallelize(MatrixUtils.matrixToRowArray(mat), 3).cache()
    }
    val bRDD = sc.parallelize(MatrixUtils.matrixToRowArray(bMat), 3).cache()

    val wsq = BlockWeightedLeastSquares.trainWithL2(
      aRDDs, bRDD, lambda, mixtureWeight, numPasses)

    val finalFullModel = wsq.xs.reduceLeft { (a, b) =>
      DenseMatrix.vertcat(a, b)
    }

    // norm(gradient) should be close to zero
    val gradient = computeGradient(fullARDD, bRDD, lambda, mixtureWeight, finalFullModel,
      wsq.bOpt.get) 

    println("norm of gradient is " + norm(gradient.toDenseVector))
    assert(Stats.aboutEq(norm(gradient.toDenseVector), 0, 1e-2))
  }

}
