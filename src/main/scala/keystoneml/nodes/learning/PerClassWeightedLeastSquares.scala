package keystoneml.nodes.learning

import keystoneml.nodes.util.VectorSplitter
import keystoneml.nodes.learning.internal.ReWeightedLeastSquaresSolver
import keystoneml.workflow.LabelEstimator

import scala.collection.mutable.ArrayBuffer

import breeze.linalg._
import breeze.numerics._
import breeze.math._
import breeze.stats._

import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner

import edu.berkeley.cs.amplab.mlmatrix.util.{Utils => MLMatrixUtils}

import keystoneml.nodes.stats.StandardScaler
import keystoneml.pipelines.Logging
import keystoneml.utils.{MatrixUtils, Stats}

/**
 * Train a weighted block-coordinate descent model using least squares
 *
 * @param blockSize size of blocks to use
 * @param numIter number of passes of co-ordinate descent to run
 * @param lambda regularization parameter
 * @param mixtureWeight how much should positive samples be weighted
 */
class PerClassWeightedLeastSquaresEstimator(
    blockSize: Int,
    numIter: Int,
    lambda: Double,
    mixtureWeight: Double,
    numFeaturesOpt: Option[Int] = None)
  extends LabelEstimator[DenseVector[Double], DenseVector[Double], DenseVector[Double]] {

  /**
   * Fit a weighted least squares model using blocks of features provided.
   * 
   * NOTE: This function makes multiple passes over the training data. Caching
   * @trainingFeatures and @trainingLabels before calling this function is recommended.
   *
   * @param trainingFeatures Blocks of training data RDDs
   * @param trainingLabels training labels RDD
   * @returns A BlockLinearMapper that contains the model, intercept
   */
  def fit(
      trainingFeatures: RDD[DenseVector[Double]],
      trainingLabels: RDD[DenseVector[Double]]): BlockLinearMapper = {
    PerClassWeightedLeastSquaresEstimator.trainWithL2(
      trainingFeatures,
      trainingLabels,
      blockSize,
      numIter,
      lambda,
      mixtureWeight,
      numFeaturesOpt)
  }
}

object PerClassWeightedLeastSquaresEstimator extends Logging {

  def trainWithL2(
      trainingFeatures: RDD[DenseVector[Double]],
      trainingLabels: RDD[DenseVector[Double]],
      blockSize: Int,
      numIter: Int,
      lambda: Double,
      mixtureWeight: Double,
      numFeaturesOpt: Option[Int]): BlockLinearMapper = {

    val nClasses = trainingLabels.first.length
    val numFeatures = numFeaturesOpt.getOrElse(trainingFeatures.first.length)
    val numExamples = trainingLabels.count

    // Calculate feature means
    val popFeatureMean = trainingFeatures.fold(DenseVector.zeros[Double](numFeatures)) { 
      (a: DenseVector[Double], b: DenseVector[Double]) =>
        a += b
      } /= numExamples.toDouble

    val (jointFeatureMean, classCounts) = computeJointFeatureMean(trainingFeatures, 
      trainingLabels, popFeatureMean, mixtureWeight, numExamples, numFeatures, nClasses)
    val weights = computeWeights(trainingLabels, numExamples, mixtureWeight, classCounts)
    val jfmMat = MatrixUtils.rowsToMatrix(jointFeatureMean.sortByKey().map(x => x._2).collect())

    // Calculate label means
    val jointLabelMean = computeJointLabelMean(classCounts, mixtureWeight, numExamples)
    val labelsZm = trainingLabels.map(x => x - jointLabelMean)

    // Split the features into blocks
    val vectorSplitter = new VectorSplitter(blockSize, Some(numFeatures))
    val trainingFeatureBlocks = vectorSplitter.apply(trainingFeatures)

    // Solve multiple single class problems and then assemble the model from each of them
    val classWiseModels = (0 until nClasses).map { classIdx =>
      val classFeatureMean = jfmMat(classIdx, ::).t

      val classLabelsZm = labelsZm.map(x => x(classIdx))
      val classWeights = weights.map(x => x(classIdx))

      val classModelBlocks = trainSingleClassWeightedLS(blockSize, trainingFeatureBlocks.size,
        numIter, lambda, numFeatures, trainingFeatureBlocks, classLabelsZm, classWeights,
        classFeatureMean)
      classModelBlocks
    }

    // classWiseModels is a Seq[Seq[DenseVector[Double]]]
    val xs = new Array[DenseMatrix[Double]](classWiseModels(0).size)
    classWiseModels.zipWithIndex.foreach { case (classModel, classIdx) =>
      classModel.zipWithIndex.foreach { case (classBlockModel, blockIdx) =>
        if(xs(blockIdx) == null) {
          xs(blockIdx) = new DenseMatrix[Double](classBlockModel.size, nClasses)
        }
        xs(blockIdx)(::, classIdx) := classBlockModel
      }
    }
    val finalFullModel = DenseMatrix.vertcat(xs:_*)
    val finalB = jointLabelMean - sum(jfmMat.t :* finalFullModel, Axis._0).t

    new BlockLinearMapper(xs, blockSize, Some(finalB))
  }

  // Compute the jointFeatureMean. Also returns the number of examples in each class
  def computeJointFeatureMean(
      features: RDD[DenseVector[Double]],
      labels: RDD[DenseVector[Double]],
      popFeatureMean: DenseVector[Double],
      mixtureWeight: Double,
      numExamples: Long,
      numFeatures: Int,
      numClasses: Int)
    : (RDD[(Int, DenseVector[Double])], Array[Int]) = {

    val sc = features.context

    val pfmBC = sc.broadcast(popFeatureMean)

    // Compute number of examples in each class
    val classCounts = labels.map { x =>
      (argmax(x), 1)
    }.reduceByKey(_ + _).collectAsMap()

    val classCountsArr = new Array[Int](numClasses)
    classCounts.foreach(x => classCountsArr(x._1) = x._2)

    // Sum feature vectors belonging to particular class
    val classFeats = features.zip(labels).map { case (feature, label) =>
      (label.toArray.indexOf(label.max), feature)
    }

    // classFeatureMeans is a RDD with k elements, each element is the mean per class
    val classFeatureSums = classFeats.foldByKey(DenseVector.zeros[Double](numFeatures))({
      (a: DenseVector[Double], b: DenseVector[Double]) => a + b
    })

    val classFeatureMeans = classFeatureSums.map { case (classKey, vectorSum) =>
      (classKey, vectorSum :/ classCounts(classKey).toDouble)
    }

    // Joint feature mean is classMean * wt + popMean * (1.0 - wt)
    val jfm = classFeatureMeans.map { case (classKey, cm) =>
      (classKey, cm*mixtureWeight + pfmBC.value*(1.0 - mixtureWeight))
    }

    (jfm, classCountsArr)
  }

  // Compute the weight matrix. Size is numExamples by numClasses
  def computeWeights(
      labels: RDD[DenseVector[Double]],
      numExamples: Long,
      mixtureWeight: Double,
      classCounts: Array[Int]): RDD[DenseVector[Double]] = {
    val weights = labels.map { thisLabel =>
      val negWt = (1.0 - mixtureWeight) / numExamples.toDouble
      val out = DenseVector.fill(thisLabel.size)(negWt)
      val thisClass = breeze.linalg.argmax(thisLabel)
      out(thisClass) += mixtureWeight / classCounts(thisClass).toDouble
      out
    }
    weights
  }

  def computeJointLabelMean(
      classCounts: Array[Int],
      mixtureWeight: Double,
      numExamples: Long): DenseVector[Double] = {
    val classMeans = new DenseVector(classCounts.map(_/numExamples.toDouble))
    val out = (classMeans:*(2.0*(1.0 - mixtureWeight))) :- 1.0
    out :+ (2.0*mixtureWeight)
  }

  // Use BCD to solve W = (X.t * (diag(B) * X) + \lambda) \ X.t * (B .* Y)
  // TODO(shivaram): Unify this with the unweighted solver ?
  def trainSingleClassWeightedLS(
      blockSize: Int,
      numBlocks: Int,
      numIter: Int,
      lambda: Double,
      numFeatures: Int,
      trainingFeatureBlocks: Seq[RDD[DenseVector[Double]]],
      classLabelsZm: RDD[Double],
      classWeights: RDD[Double],
      classMean: DenseVector[Double]): Seq[DenseVector[Double]] = {

    val (model, residual) = ReWeightedLeastSquaresSolver.trainWithL2(
      blockSize,
      numBlocks,
      numIter,
      lambda,
      numFeatures,
      1, // numClasses
      trainingFeatureBlocks,
      classLabelsZm.map(x => DenseVector(x)),
      classWeights,
      classMean)

    model.map(x => x(::, 0))
  }
}
