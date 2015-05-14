package nodes.learning

import scala.collection.mutable.ArrayBuffer

import breeze.linalg._
import breeze.numerics._
import breeze.math._
import breeze.stats._

import org.apache.spark.rdd.RDD

import edu.berkeley.cs.amplab.mlmatrix.{RowPartition, NormalEquations, BlockCoordinateDescent, RowPartitionedMatrix}
import edu.berkeley.cs.amplab.mlmatrix.util.{Utils => MLMatrixUtils}

import nodes.misc.StandardScaler
import pipelines.{Transformer, Logging}
import utils.{MatrixUtils, Stats}

object BlockWeightedLeastSquares extends Logging {
 
  // Utility class that holds statistics related to each block we solve
  // Used to cache information across many passes
  case class BlockStatistics(popCov: DenseMatrix[Double], popMean: DenseVector[Double],
    jointMean: DenseMatrix[Double], jointMeanRDD: RDD[DenseVector[Double]])

  /**
   * Train a weighted block-coordinate descent model using least squares
   * NOTE: This function assumes that the trainingFeatures have been partitioned by
   * their class index. i.e. each partition of training data contains data for a single class
   *
   * NOTE: This function makes multiple passes over the training data. Caching 
   * @trainingFeatures and @trainingLabels before calling this function is recommended.
   * 
   * @param trainingFeatures Blocks of training data RDDs
   * @param trainingLabels training labels RDD
   * @param lambda regularization parameter
   * @param mixtureWeight how much should positive samples be weighted
   * @param numPasses number of passes of co-ordinate descent to run
   *
   * @returns A BlockLinearMapper that contains the model, intercept
   */
  def trainWithL2(
      trainingFeatures: Seq[RDD[DenseVector[Double]]],
      trainingLabels: RDD[DenseVector[Double]],
      lambda: Double,
      mixtureWeight: Double,
      numPasses: Int): BlockLinearMapper = {
    val sc = trainingFeatures.head.context

    // Check if all examples in a partition are of the same class
    val sameClasses = trainingLabels.mapPartitions { iter =>
      Iterator.single(iter.map(label => label.data.indexOf(label.max)).toSeq.distinct.length == 1)
    }.collect()
    require(sameClasses.forall(x => x), "partitions should contain elements of the same class")

    val classIdxs = trainingLabels.mapPartitions { iter =>
      Iterator.single(iter.map(label => label.toArray.indexOf(label.max)).toSeq.distinct.head)
    }.cache().setName("classIdx")
    
    val nTrain = trainingLabels.count
    val nClasses = trainingLabels.first.length
    val numBlocks = trainingFeatures.length

    val trainingLabelsMat = trainingLabels.mapPartitions(part =>
      Iterator.single(MatrixUtils.rowsToMatrix(part)))

    val jointLabelMean = DenseVector(trainingLabelsMat.map { part =>
      2*mixtureWeight + (2*(1.0-mixtureWeight)* part.rows/nTrain.toDouble) -1
    }.collect():_*)

    // Initialize models to zero here. Each model is a (W, b)
    // NOTE: We get first element from every training block here
    val models = (0 until numBlocks).map { block =>
      val blockSize = trainingFeatures(block).first.length
      DenseMatrix.zeros[Double](blockSize, nClasses)
    }.toArray

    // Initialize residual to labels - jointLabelMean
    var residual = trainingLabelsMat.map { mat =>
      mat(*, ::) :- jointLabelMean
    }.cache().setName("residual")

    var residualMean = MLMatrixUtils.treeReduce(residual.map { mat =>
      mean(mat(::, *)).toDenseVector
    }, MatrixUtils.addVectors) :/ nClasses.toDouble

    @transient val blockStats: Array[Option[BlockStatistics]] = (0 until numBlocks).map { blk =>
      None
    }.toArray

    (0 until numPasses).foreach { pass =>
      var blockIdx = 0
       // TODO: Figure out if this should be shuffled ? rnd.shuffle((0 until numBlocks).toList)
      val randomBlocks = (0 until numBlocks).toList
      while (blockIdx < numBlocks) {
        val block = randomBlocks(blockIdx)
        logInfo("Running pass " + pass + " block " + block)
        val blockFeatures = trainingFeatures(block)

        val blockFeaturesMat = blockFeatures.mapPartitions { part => 
          Iterator.single(MatrixUtils.rowsToMatrix(part))
        }.cache().setName("blockFeaturesMat")

        val treeBranchingFactor = sc.getConf.getInt("spark.mlmatrix.treeBranchingFactor", 2).toInt
        val depth = math.ceil(math.log(blockFeaturesMat.partitions.size) / 
          math.log(treeBranchingFactor)).toInt

        val (popCov, popXTR, jointMeansRDD, popMean) = if (pass == 0) {
          // Step 1: Calculate population mean, covariance
          // TODO: This expects blockFeatures to be cached as this does one pass ??
          val blockPopMean = new StandardScaler(normalizeStdDev=false).fit(blockFeatures).mean

          // This is 1000x4k -- So keep a RDD version of it that we can zip with each partition
          // and also a local version of it.
          val blockJointMeansRDD = blockFeaturesMat.map { mat =>
            mean(mat(::, *)).toDenseVector * mixtureWeight + blockPopMean * (1.0 -
              mixtureWeight)
          }.cache().setName("jointMeans")
          val blockJointMeans = MatrixUtils.rowsToMatrix(blockJointMeansRDD.collect())

          val aTaResidual = MLMatrixUtils.treeReduce(blockFeaturesMat.zip(residual).map { part =>
            (part._1.t * part._1, part._1.t * part._2)
          }, MatrixUtils.addPairMatrices, depth=depth)

          val blockPopCov = (aTaResidual._1 :/ nTrain.toDouble) - (blockPopMean * blockPopMean.t)

          blockStats(block) =
            Some(BlockStatistics(blockPopCov, blockPopMean, blockJointMeans, blockJointMeansRDD))

          (blockPopCov, aTaResidual._2 :/ (nTrain.toDouble), blockJointMeansRDD, blockPopMean)
        } else {
          val aTResidual = MLMatrixUtils.treeReduce(blockFeaturesMat.zip(residual).map { part =>
            part._1.t * part._2
          }, MatrixUtils.addMatrices, depth=depth)

          val blockStat = blockStats(block).get 
          (blockStat.popCov, aTResidual :/ (nTrain.toDouble), blockStat.jointMeanRDD,
            blockStat.popMean)
        }

        val popCovBC = sc.broadcast(popCov)
        val popMeanBC = sc.broadcast(popMean)
        val popXTRBC = sc.broadcast(popXTR)
        val modelBC = sc.broadcast(models(block))

        val modelsThisPass = blockFeaturesMat.zip(residual.zip(jointMeansRDD.zip(classIdxs))).map {
            case (featuresLocal, secondPart) =>
          val (classJointMean, classIdx) = secondPart._2
          val resLocal = secondPart._1(::, classIdx)
          // compute the number of examples in this class
          val numPosEx = featuresLocal.rows
          // compute the mean and covariance of the features in this class
          val classMean = mean(featuresLocal(::, *)).toDenseVector
          val classFeatures_ZM = featuresLocal(*, ::) :- classMean
          val classCov = (classFeatures_ZM.t * classFeatures_ZM) :/ numPosEx.toDouble
          val classXTR = (featuresLocal.t * resLocal) :/ numPosEx.toDouble
       
          val popCovMat = popCovBC.value
          val popMeanVec = popMeanBC.value
          val popXTRMat = popXTRBC.value

          val meanDiff = classMean - popMeanVec

          val jointXTX = popCovMat * (1.0 - mixtureWeight) +
            classCov * mixtureWeight +
            meanDiff * meanDiff.t * (1.0 - mixtureWeight) * mixtureWeight

          val meanMixtureWt: Double = (residualMean(classIdx) * (1.0 - mixtureWeight) +
            mixtureWeight * mean(resLocal))

          val jointXTR = popXTRMat(::, classIdx) * (1.0 - mixtureWeight) +
            classXTR.toDenseVector * mixtureWeight -
            classJointMean * meanMixtureWt

          val numDims = jointXTX.cols

          val W = (jointXTX + (DenseMatrix.eye[Double](numDims) :* lambda) ) \ (jointXTR -
            modelBC.value(::, classIdx) * lambda)

          W.toDenseMatrix.t
        }.collect()

        // TODO: Write a more reasonable conversion function here.
        val localFullModel = modelsThisPass.reduceLeft { (a, b) =>
          DenseMatrix.horzcat(a, b)
        }

        // So newXj is oldXj + localFullModel
        models(block) += localFullModel
        val localFullModelBC = sc.broadcast(localFullModel)

        val newResidual = blockFeaturesMat.zip(residual).map { part =>
          part._2 -= part._1 * localFullModelBC.value 
          part._2
        }.cache().setName("residual")

        newResidual.count
        residual.unpersist()
        residual = newResidual

        residualMean = residual.map { mat =>
          mean(mat(::, *)).toDenseVector
        }.reduce(MatrixUtils.addVectors) :/ nClasses.toDouble

        popCovBC.unpersist()
        popMeanBC.unpersist()
        popXTRBC.unpersist()
        modelBC.unpersist()
        localFullModelBC.unpersist()

        blockFeaturesMat.unpersist()

        sc.parallelize(0 until sc.getExecutorMemoryStatus.size,
          sc.getExecutorMemoryStatus.size).foreach { x =>
          System.gc()
        }

        blockIdx = blockIdx + 1
      }
    }

    // Takes the local models stacks them vertically to get a full model
    val finalFullModel = models.reduceLeft { (a, b) =>
      DenseMatrix.vertcat(a, b)
    }

    val jointMeansCombined = blockStats.map(_.get.jointMean).reduceLeft { (a, b) =>
      DenseMatrix.horzcat(a, b)
    }
    val finalB = jointLabelMean - sum(jointMeansCombined.t :* finalFullModel, Axis._0).toDenseVector
    new BlockLinearMapper(models, Some(finalB))
  }

}
