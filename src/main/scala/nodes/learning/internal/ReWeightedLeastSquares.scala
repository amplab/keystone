package nodes.learning.internal

import scala.collection.mutable.ArrayBuffer

import breeze.linalg._
import breeze.numerics._
import breeze.math._
import breeze.stats._

import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner

import edu.berkeley.cs.amplab.mlmatrix.util.{Utils => MLMatrixUtils}

import pipelines.Logging
import utils.{MatrixUtils, Stats}

object ReWeightedLeastSquaresSolver extends Logging {

  /**
   * Use BCD to solve W = (X.t * (diag(B) * X) + \lambda * I) \ X.t * (B .* Y)
   * 
   * @param blockSize blockSize to use for Block Coordinate Descent
   * @param numBlocks number of blocks in the input
   * @param numIter number of iterations of BCD to run
   * @param lambda L2 regularization parameter
   * @param numFeatures number of features in the input (columns in X)
   * @param numClasses number of classes (columns in Y)
   * @param trainingFeatureBlocks blocks of input features
   * @param labelsZm zero-mean'd labels matrix
   * @param weights diagonal values for the weights matrix
   * @param featureMean mean to be subtracted from X before the solve
   *
   * @returns model (W) split as blocks and the final residual (X*W)
   */
  def trainWithL2(
      blockSize: Int,
      numBlocks: Int,
      numIter: Int,
      lambda: Double,
      numFeatures: Int,
      numClasses: Int,
      trainingFeatureBlocks: Seq[RDD[DenseVector[Double]]],
      labelsZm: RDD[DenseVector[Double]],
      weights: RDD[Double],
      featureMean: DenseVector[Double])
    : (Seq[DenseMatrix[Double]], RDD[DenseMatrix[Double]]) = {

    val labelsZmMat = labelsZm.mapPartitions { iter =>
      Iterator.single(MatrixUtils.rowsToMatrix(iter))
    }
    val weightsMat = weights.mapPartitions { iter =>
      Iterator.single(DenseVector(iter.toArray))
    }

    var residual = labelsZmMat.map { l =>
      DenseMatrix.zeros[Double](l.rows, l.cols)
    }.cache()

    // Initialize model to blockSize. This will be resized if its different
    // inside the solve loop
    val model = (0 until numBlocks).map { block =>
      DenseMatrix.zeros[Double](blockSize, numClasses)
    }.toArray

    val treeBranchingFactor = weights.context.getConf.getInt(
      "spark.mlmatrix.treeBranchingFactor", 2).toInt
    val depth = math.max(math.ceil(math.log(labelsZmMat.partitions.size) /
        math.log(treeBranchingFactor)).toInt, 1)
    val aTaCache = new Array[DenseMatrix[Double]](numBlocks)

    (0 until numIter).foreach { pass =>
      (0 until numBlocks).foreach { block =>
        val aPart = trainingFeatureBlocks(block)
        // Zero-mean the training features
        val featureMeanBlock = featureMean(
          block * blockSize until min((block+1) * blockSize, numFeatures))
        val aPartMatZm = aPart.mapPartitions { iter =>
          val mat = MatrixUtils.rowsToMatrix(iter)
          Iterator.single( (mat(*, ::) - featureMeanBlock) )
        }.cache().setName("aPartMatZm")
        aPartMatZm.count
      
        // Compute X.t * (diag(B) * X) if this the first iteration
        if (pass == 0) {
          val aTaComputed = MLMatrixUtils.treeReduce(aPartMatZm.zip(weightsMat).map { x =>
            // Multiplication by diagonal is same as hadamard product per column
            x._1.t * ( x._1(::, *) :* x._2 )
          }, (a: DenseMatrix[Double], b: DenseMatrix[Double]) => a += b, depth=depth)
          aTaCache(block) = aTaComputed
          model(block) = DenseMatrix.zeros[Double](aTaComputed.rows, numClasses)
        }

        val aTaBlock = aTaCache(block)
        val modelBC = weightsMat.context.broadcast(model(block))

        val aTbBlock = MLMatrixUtils.treeReduce(
          aPartMatZm.zip(labelsZmMat.zip(residual.zip(weightsMat))).map { x =>
            val featPart = x._1
            val labelPart = x._2._1
            val resPart = x._2._2._1
            val weightPart = x._2._2._2
           
            // TODO(shivaram): This might generate a lot of GC ?
            
            // Remove B.*(X * wOld) from the residual
            val xWOld = (featPart * modelBC.value)
            val resUpdated = resPart - ( xWOld(::, *) :* weightPart )
           
            // Compute X.t * ((B .* Y) - residual)
            val aTbPart = featPart.t * ((labelPart(::, *) :* weightPart) - resUpdated)
            aTbPart
          }, (a: DenseMatrix[Double], b: DenseMatrix[Double]) => a += b, depth = depth)

        val newModel = (aTaBlock + (DenseMatrix.eye[Double](aTaBlock.rows) * lambda)) \ aTbBlock
        val newModelBC = weights.context.broadcast(newModel)

        model(block) = newModel

        // Update the residual by adding B.*(X * wNew) and subtracting B.*(X * wOld)
        val newResidual = aPartMatZm.zip(residual.zip(weightsMat)).map { part =>
          val diffModel = newModelBC.value - modelBC.value
          val xWDiff = (part._1 * diffModel)
          part._2._1 += (xWDiff(::, *) :* part._2._2)
          part._2._1
        }.cache().setName("residual")
        newResidual.count
        residual.unpersist()
        residual = newResidual

        aPartMatZm.unpersist()
      }
    }
    (model, residual)
  }
}
