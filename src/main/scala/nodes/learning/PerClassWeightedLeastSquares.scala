package nodes.learning

import nodes.util.VectorSplitter
import workflow.LabelEstimator

import scala.collection.mutable.ArrayBuffer

import breeze.linalg._
import breeze.numerics._
import breeze.math._
import breeze.stats._

import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner

import edu.berkeley.cs.amplab.mlmatrix.util.{Utils => MLMatrixUtils}

import nodes.stats.StandardScaler
import pipelines.Logging
import utils.{MatrixUtils, Stats}

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
      val classFeatureMean = jointFeatureMean.filter { x => 
        x._1 == classIdx
      }.map(x => x._2).collect()(0)

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
    val finalB = jointLabelMean - sum(jfmMat.t :* finalFullModel, Axis._0).toDenseVector

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

    val classLabelsZmMat = classLabelsZm.mapPartitions { iter =>
      Iterator.single(DenseVector(iter.toArray))
    }
    val classWeightsMat = classWeights.mapPartitions { iter =>
      Iterator.single(DenseVector(iter.toArray))
    }

    var residual = classLabelsZmMat.map { l =>
      DenseVector.zeros[Double](l.size)
    }.cache()

    // Initialize model to blockSize. This will be resized if its different
    // inside the solve loop
    val model = (0 until numBlocks).map { block =>
      DenseVector.zeros[Double](blockSize)
    }.toArray

    val treeBranchingFactor = classWeights.context.getConf.getInt(
      "spark.mlmatrix.treeBranchingFactor", 2).toInt
    val depth = math.max(math.ceil(math.log(classLabelsZmMat.partitions.size) /
        math.log(treeBranchingFactor)).toInt, 1)
    val aTaCache = new Array[DenseMatrix[Double]](numBlocks)

    (0 until numIter).foreach { pass =>
      (0 until numBlocks).foreach { block =>
        val aPart = trainingFeatureBlocks(block)
        // Zero-mean the training features
        val classMeanBlock = classMean(
          block * blockSize until min((block+1) * blockSize, numFeatures))
        val aPartMatZm = aPart.mapPartitions { iter =>
          val mat = MatrixUtils.rowsToMatrix(iter)
          Iterator.single( (mat(*, ::) - classMeanBlock) )
        }.cache().setName("aPartMatZm")
        aPartMatZm.count
      
        // Compute X.t * (diag(B) * X) if this the first iteration
        if (pass == 0) {
          val aTaComputed = MLMatrixUtils.treeReduce(aPartMatZm.zip(classWeightsMat).map { x =>
            // Multiplication by diagonal is same as hadamard product per column
            x._1.t * ( x._1(::, *) :* x._2 )
          }, (a: DenseMatrix[Double], b: DenseMatrix[Double]) => a += b, depth=depth)
          aTaCache(block) = aTaComputed
          model(block) = DenseVector.zeros[Double](aTaComputed.rows)
        }

        val aTaBlock = aTaCache(block)
        val modelBC = classWeightsMat.context.broadcast(model(block))

        val aTbBlock = MLMatrixUtils.treeReduce(
          aPartMatZm.zip(classLabelsZmMat.zip(residual.zip(classWeightsMat))).map { x =>
            val featPart = x._1
            val labelPart = x._2._1
            val resPart = x._2._2._1
            val weightPart = x._2._2._2
           
            // TODO(shivaram): This might generate a lot of GC ?
            
            // Remove B.*(X * wOld) from the residual
            val resUpdated = resPart - (weightPart :* (featPart * modelBC.value))
           
            // Compute X.t * ((B .* Y) - residual)
            val aTbPart = featPart.t * ((weightPart :* labelPart) - resUpdated)
            aTbPart 
          }, (a: DenseVector[Double], b: DenseVector[Double]) => a += b, depth = depth)

        val newModel = (aTaBlock + (DenseMatrix.eye[Double](aTaBlock.rows) * lambda)) \ aTbBlock
        val newModelBC = classWeights.context.broadcast(newModel)

        model(block) = newModel

        // Update the residual by adding B.*(X * wNew) and subtracting B.*(X * wOld)
        val newResidual = aPartMatZm.zip(residual.zip(classWeightsMat)).map { part =>
          val diffModel = newModelBC.value - modelBC.value
          part._2._1 += (part._2._2 :* (part._1 * diffModel))
          part._2._1
        }.cache().setName("residual")
        newResidual.count
        residual.unpersist()
        residual = newResidual

        aPartMatZm.unpersist()
      }
    }
    model
  }
}
