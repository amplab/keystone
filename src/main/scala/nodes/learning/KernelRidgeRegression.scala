package nodes.learning

import scala.reflect.ClassTag
import scala.util.Random
import scala.collection.mutable.ListBuffer

import breeze.linalg._
import breeze.math._
import breeze.numerics._

import edu.berkeley.cs.amplab.mlmatrix.RowPartitionedMatrix
import edu.berkeley.cs.amplab.mlmatrix.util.{Utils => MLMatrixUtils}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import pipelines.Logging
import workflow.LabelEstimator
import utils._

/**
 * Solves a kernel ridge regression problem of the form
 * (K(x, x) + \lambda * I) * W = Y
 * using Gauss-Seidel based Block Coordinate Descent.
 *
 * The function K is specified by the kernel generator and this class
 * uses the dual formulation of the ridge regression objective to improve
 * numerical stability.
 *
 * @param kernelGenerator kernel function to apply to create kernel matrix
 * @param lambda L2 regularization value
 * @param blockSize number of columns in each block of BCD
 * @param numEpochs number of epochs of BCD to run
 * @param blockPermuter seed used for permuting column blocks in BCD
 * @param blocksBeforeCheckpoint frequency at which intermediate data should be checkpointed
 */
class KernelRidgeRegression[T: ClassTag](
    kernelGenerator: KernelGenerator[T],
    lambda: Double,
    blockSize: Int,
    numEpochs: Int,
    blockPermuter: Option[Long] = None,
    blocksBeforeCheckpoint: Int = 25)
  extends LabelEstimator[T, DenseVector[Double], DenseVector[Double]] {

  override def fit(
      data: RDD[T],
      labels: RDD[DenseVector[Double]]): KernelBlockLinearMapper[T] = {
    val kernelTransformer = kernelGenerator.fit(data)
    val trainKernelMat = kernelTransformer(data)
    val nTrain = data.count

    val wLocals = KernelRidgeRegression.trainWithL2(
      trainKernelMat,
      labels,
      lambda,
      blockSize,
      numEpochs,
      blockPermuter,
      blocksBeforeCheckpoint)

    new KernelBlockLinearMapper(wLocals, blockSize, kernelTransformer, nTrain,
      blocksBeforeCheckpoint)
  }
}

object KernelRidgeRegression extends Logging {
  /**
   * Solves a linear system of the form (K + \lambda * I) * W = Y
   * using Gauss-Seidel based Block Coordinate Descent as described in
   * http://arxiv.org/abs/1602.05310
   *
   * K is assumed to be a symmetric kernel matrix generated using a kernel
   * generator.
   *
   * @param data the kernel matrix to use
   * @param labels training labels RDD
   * @param lambda L2 regularization parameter
   * @param blockSize number of columns per block of Gauss-Seidel solve
   * @param numEpochs number of passes of co-ordinate descent to run
   * @param blockPermuter seed to use for permuting column blocks
   * @param blocksBeforeCheckpoint frequency at which intermediate data should be checkpointed
   *
   * @return a model that can be applied on test data.
   */
  def trainWithL2[T: ClassTag](
      trainKernelMat: KernelMatrix,
      labels: RDD[DenseVector[Double]],
      lambda: Double,
      blockSize: Int,
      numEpochs: Int,
      blockPermuter: Option[Long],
      blocksBeforeCheckpoint: Int = 25): Seq[DenseMatrix[Double]] = {

    val nTrain = labels.count.toInt
    val nClasses = labels.first.length

    // Currently we only support one lambda but the code
    // but the code is structured to support multiple lambdas
    val lambdas = IndexedSeq(lambda)
    val numBlocks = math.ceil(nTrain.toDouble/blockSize).toInt

    val preFixLengths = labels.mapPartitions { part =>
      Iterator.single(part.length)
    }.collect().scanLeft(0)(_+_)
    val preFixLengthsBC = labels.context.broadcast(preFixLengths)

    // Convert each partition from Iterator[DenseVector[Double]] to
    // a single DenseMatrix[Double]. Also cache this as n*k should be small.
    val labelsMat = labels.mapPartitions { part =>
      MatrixUtils.rowsToMatrixIter(part)
    }.setName("labelsMat").cache()
    labelsMat.count
    val labelsRPM = RowPartitionedMatrix.fromMatrix(labelsMat)

    // Model is same size as labels
    // NOTE: We create one RDD with a array of matrices as its cheaper
    // to compute residuals that way
    var model = labelsMat.map { x =>
      lambdas.map { l =>
        DenseMatrix.zeros[Double](x.rows, x.cols)
      }
    }.setName("model").cache()
    model.count

    // We also keep a local copy of the model to use computing residual
    // NOTE: The last block might have fewer than blockSize features
    // but we will correct this inside the solve
    val wLocals = lambdas.map { l =>
      (0 until numBlocks).map { x =>
        DenseMatrix.zeros[Double](blockSize, nClasses)
      }.to[ListBuffer]
    }
    val blockShuffler = blockPermuter.map(seed => new Random(seed))

    (0 until numEpochs).foreach { pass =>
      val inOrder = (0 until numBlocks).toIndexedSeq
      val blockOrder = blockShuffler.map(rnd => rnd.shuffle(inOrder)).getOrElse(inOrder)

      blockOrder.foreach { block =>
        val blockBegin = System.nanoTime
        // Use the kernel block generator to get kernel matrix for these column blocks
        val blockIdxs = (blockSize * block) until (math.min(nTrain, (block + 1) * blockSize))
        val blockIdxsSeq = blockIdxs.toArray
        val blockIdxsBC = labelsMat.context.broadcast(blockIdxsSeq)

        val kernelBlockMat = trainKernelMat(blockIdxsSeq)
        val kernelBlockBlockMat = trainKernelMat.diagBlock(blockIdxsSeq)

        val kernelGenEnd = System.nanoTime

        // Build up the residual
        val treeBranchingFactor = labels.context.getConf.getInt(
          "spark.mlmatrix.treeBranchingFactor", 2).toInt
        val depth = max(math.ceil(math.log(kernelBlockMat.partitions.size) /
          math.log(treeBranchingFactor)).toInt, 1)

        // Compute K_B^T * W as its easy to do this.
        // After this we will subtract out K_BB^T * W_B
        // b x k
        val residuals = MLMatrixUtils.treeReduce(kernelBlockMat.zip(model).map { x =>
          x._2.map { y =>
            // this is a b * n1 times n1 * k
            x._1.t * y
          }
        }, MatrixUtils.addMatrices, depth=depth)

        val residualEnd = System.nanoTime

        // This is b*k
        val y_bb = labelsRPM(blockIdxs, ::).collect()

        val collectEnd = System.nanoTime

        // This is a tuple of (oldBlockBC, newBlockBC)
        val wBlockBCs = (0 until lambdas.length).map { l =>
          // This is b*k
          val wBlockOld = if (pass == 0) {
            DenseMatrix.zeros[Double](blockIdxs.size, nClasses)
          } else {
            wLocals(l)(block)
          }
          val lhs = kernelBlockBlockMat +
            DenseMatrix.eye[Double](kernelBlockBlockMat.rows) * lambdas(l)
          // Subtract out K_bb * W_bb from residual
          val rhs = y_bb - (residuals(l) - kernelBlockBlockMat.t * wBlockOld)

          val wBlockNew = lhs \ rhs
          wLocals(l)(block) = wBlockNew
          val wBlockOldBC = labels.context.broadcast(wBlockOld)
          val wBlockNewBC = labels.context.broadcast(wBlockNew)
          (wBlockOldBC, wBlockNewBC)
        }

        val localSolveEnd = System.nanoTime

        var newModel = updateModel(
          model, wBlockBCs.map(_._2), blockIdxsBC, preFixLengthsBC).cache()
        // This is to truncate the lineage every 50 blocks
        if (labels.context.getCheckpointDir.isDefined &&
            block % blocksBeforeCheckpoint == (blocksBeforeCheckpoint - 1)) {
          newModel = MatrixUtils.truncateLineage(newModel, false)
        }

        // materialize the new model
        newModel.count()

        model.unpersist(true)
        model = newModel

        val updateEnd = System.nanoTime

        logInfo(s"EPOCH_${pass}_BLOCK_${block} took " +
          ((System.nanoTime - blockBegin)/1e9) + " seconds")

        logInfo(s"EPOCH_${pass}_BLOCK_${block} " +
          s"kernelGen: ${(kernelGenEnd - blockBegin)/1e9} " +
          s"residual: ${(residualEnd - kernelGenEnd)/1e9} " +
          s"collect: ${(collectEnd - residualEnd)/1e9} " +
          s"localSolve: ${(localSolveEnd - collectEnd)/1e9} " +
          s"modelUpdate: ${(updateEnd - localSolveEnd)/1e9}")

        trainKernelMat.unpersist(blockIdxsSeq)

        wBlockBCs.map { case (wBlockOldBC, wBlockNewBC) =>
          wBlockOldBC.unpersist(true)
          wBlockNewBC.unpersist(true)
        }
        blockIdxsBC.unpersist(true)
      }
    }
    labelsMat.unpersist(true)
    preFixLengthsBC.unpersist(true)
    wLocals(0)
  }

  def updateModel(
      model: RDD[IndexedSeq[DenseMatrix[Double]]],
      wBlockNewBC: Seq[Broadcast[DenseMatrix[Double]]],
      blockIdxsBC: Broadcast[Array[Int]],
      preFixLengthsBC: Broadcast[Array[Int]]): RDD[IndexedSeq[DenseMatrix[Double]]] = {
    val newModel = model.mapPartitionsWithIndex { case (idx, part) =>
      // note that prefix length is *not* cumsum (so the first entry is 0)
      val partBegin = preFixLengthsBC.value(idx)
      val wParts = part.next
      assert(part.isEmpty)
      wParts.zipWithIndex.foreach { case (wPart, idx) =>
        val partLength = wPart.rows

        // [partBegin, partBegin + partLength)
        // [blockIdxs[0], blockIdxs[-1]]
        //
        // To compute which indices of the model to update, we take two
        // ranges, map them both to example space ([nTrain]) so that we can
        // intersect them, and then map the intersection back the delta model
        // index space and partition index space.
        val responsibleRange = (partBegin until (partBegin + partLength)).toSet
        val inds = blockIdxsBC.value.zipWithIndex.filter { case (choice, _) =>
          responsibleRange.contains(choice)
        }
        val partInds = inds.map(x => x._1 - partBegin).toSeq
        val blockInds = inds.map(x => x._2).toSeq

        val wBlockNewBCvalue = wBlockNewBC(idx).value
        wPart(partInds, ::) := wBlockNewBCvalue(blockInds, ::)
      }
      Iterator.single(wParts)
    }
    newModel
  }
}
