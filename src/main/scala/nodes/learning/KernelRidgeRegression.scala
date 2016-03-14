package nodes.learning

import workflow.LabelEstimator

import breeze.linalg._
import breeze.math._
import breeze.numerics._
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import utils._


import edu.berkeley.cs.amplab.mlmatrix.RowPartitionedMatrix
import edu.berkeley.cs.amplab.mlmatrix.util.{Utils => MLMatrixUtils}

import scala.util.Random

class KernelRidgeRegression(
  kernelBlockGenerator: KernelGenerator,
  lambda: Double,
  blockSize: Int,
  numEpochs: Int,
  blockPermuter: Option[Long] = None,
  cacheKernel: Boolean = true,
  stopAfterBlocks: Option[Int] = None)
  extends LabelEstimator[DenseVector[Double], DenseVector[Double], DenseVector[Double]] {

  /**
   * Fit a kernel ridge regression model using provided data, labels
   */
  override  def fit(data: RDD[DenseVector[Double]],
          labels: RDD[DenseVector[Double]]): KernelBlockModel = {
    KernelRidgeRegression.trainWithL2(data,
      labels,
      kernelBlockGenerator,
      lambda,
      blockSize,
      numEpochs,
      blockPermuter,
      cacheKernel,
      stopAfterBlocks)
  }
  }


object KernelRidgeRegression {

  /**
   * @param trainingFeatures Blocks of training data RDDs
   * @param labels training labels RDD
   * @param lambdas regularization parameter
   * @param blockSize number of columns per block of Gauss-Seidel solve
   * @param numEpochs number of passes of co-ordinate descent to run
   * Returns (W, B, fobj)
   */
  def trainWithL2(
      data: RDD[DenseVector[Double]],
      labels: RDD[DenseVector[Double]],
      kernelBlockGenerator: KernelGenerator,
      lambda: Double,
      blockSize: Int,
      numEpochs: Int,
      blockPermuter: Option[Long],
      cacheKernel: Boolean = false,
      stopAfterBlocks: Option[Int] = None): KernelBlockModel = {

    //val sc = labels.context
    val nTrain = labels.count.toInt
    val nClasses = labels.first.length


    /* Currently we only support one lambda but the code
     * but the code is structured to support multiple lambdas
     * with ease
     */

    val lambdas = Array(lambda)

    val numBlocks = math.ceil(nTrain.toDouble/blockSize).toInt
    println(s"numBlocks $numBlocks blockSize $blockSize")

    val preFixLengths = labels.mapPartitions { part =>
      Iterator.single(part.length)}.collect().scanLeft(0)(_+_)
    val preFixLengthsBC = labels.context.broadcast(preFixLengths)
    println(s"preFixLengths ${preFixLengths.toSeq}")

    // Convert each partition from Iterator[DenseVector[Double]] to
    // a single DenseMatrix[Double]
    //
    // Cache this as n*k should be small
    // TODO: Should we zero mean the labels ?
    val labelsMat = labels.mapPartitions { part =>
      MatrixUtils.rowsToMatrixIter(part)
    }.cache()
    labelsMat.count
    val labelsRPM = RowPartitionedMatrix.fromMatrix(labelsMat)

    val kWs = lambdas.map { l =>
      labelsMat.map { part =>
        val nrows = part.rows
        val ncols = part.cols
        DenseMatrix.zeros[Double](nrows, ncols)
      }.cache()
    }

    val normWSquares = Array.fill(lambdas.length)(0.0)

    // Model is same size as labels
    // NOTE: We create one RDD with a array of matrices as its cheaper
    // to compute residuals that way
    var models = labelsMat.map { x =>
      lambdas.map { l =>
        DenseMatrix.zeros[Double](x.rows, x.cols)
      }
    }.cache()
    models.count

    // We also keep a local copy of the model to use computing residual
    // NOTE: The last block might have fewer than blockSize features
    // but we will correct this inside the solve
    val wLocals = lambdas.map { l =>
      (0 until numBlocks).map { x =>
        DenseMatrix.zeros[Double](blockSize, nClasses)
      }.toArray
    }


    val intercept = DenseVector.zeros[Double](nClasses)
    var curObjs = Array.fill(lambdas.length)(0.0)

    val blockShuffler = blockPermuter.map(seed => new Random(seed))

    val kernelBlockCache = new Array[RDD[DenseMatrix[Double]]](numBlocks)
    val kernelBBCache = new Array[DenseMatrix[Double]](numBlocks)

    (0 until numEpochs).foreach { pass =>
      val epochBegin = System.nanoTime

      // NOTE: We can shuffle blocks here if we want to
      val inOrder = (0 until numBlocks).toIndexedSeq
      val blockOrder = blockShuffler.map(rnd => rnd.shuffle(inOrder)).getOrElse(inOrder)

      val blockOrderTrunc = stopAfterBlocks.map(x => blockOrder.take(x)).getOrElse(blockOrder)

      blockOrderTrunc.foreach { block =>

        val blockBegin = System.nanoTime

        // Use the kernel block generator to get kernel matrix for these column blocks
        val blockIdxs = (blockSize * block) until (math.min(nTrain, (block + 1) * blockSize))
        val blockIdxsSeq = blockIdxs.toArray
        val blockIdxsBC = labels.context.broadcast(blockIdxsSeq)

        val kernelBegin = System.nanoTime
        val (kernelBlockMat, k_bb) = if (cacheKernel && pass != 0) {
            (kernelBlockCache(block), kernelBBCache(block))
          } else {

            // Columns corresponding to block b of the data
            val kernelBlock = kernelBlockGenerator(data, blockIdxs.toArray, true, true)
            val kernelBlockBlockComputed =
              kernelBlockGenerator.cachedBlockBlock.map { cache =>
              /* Make sure we are getting data for the right block */
              assert(blockIdxs.toArray.deep == cache._2.toArray.deep)
              /* Clear cache */
              kernelBlockGenerator.cachedBlockBlock = None
              cache._1 }.getOrElse {
                /* Redoing annoying work */
                val blockData = data.zipWithIndex.filter{ case (vec, idx) =>
                  blockIdxs.contains(idx)
                }.map(x=> x._1)
                MatrixUtils.rowsToMatrix(kernelBlockGenerator(blockData).collect())
              }



            // Convert to a matrix form and cache this RDD
            // Since this is n*b we should be fine.
            val kernelBlockMatComputed = kernelBlock.mapPartitions { part =>
              Iterator.single(MatrixUtils.rowsToMatrix(part))
            }.cache()
            kernelBlockMatComputed.count
            kernelBlock.unpersist()
            if (cacheKernel) {
              kernelBlockCache(block) = kernelBlockMatComputed
              kernelBBCache(block) = kernelBlockBlockComputed
            }
            (kernelBlockMatComputed, kernelBlockBlockComputed)
          }

        val kernelTime = timeElasped(kernelBegin)

        // TODO: Should we zero mean the kernel block ?
        // Build up the residual
        val treeBranchingFactor = labels.context.getConf.getInt("spark.mlmatrix.treeBranchingFactor", 2).toInt
        val depth = max(math.ceil(math.log(kernelBlockMat.partitions.size) /
          math.log(treeBranchingFactor)).toInt, 1)

        val residualBegin = System.nanoTime

        // Compute K_B^T * W as its easy to do this.
        // After this we will subtract out K_BB^T * W_B
        // b x k

        val residuals = MLMatrixUtils.treeReduce(kernelBlockMat.zip(models).map { x =>
          x._2.map { y =>
            // this is a b * n1 times n1 * k
            // gives b * k
            x._1.t * y
          }
        }, addMatrixArrays, depth=depth)

        //val residual = kernelBlockMat.zip(model).map { case (lhs, rhs) => lhs.t * rhs }.reduce(_+_)

        val residualTime = timeElasped(residualBegin)
        val collectBegin = System.nanoTime

        // This is b*k
        val y_bb = labelsRPM(blockIdxs, ::).collect()

        val collectTime = timeElasped(collectBegin)
        val localSolveBegin = System.nanoTime

        // This is a tuple of (oldBlockBC, newBlockBC)
        val wBlockBCs = (0 until lambdas.length).map { l =>
          // This is b*k
          val wBlockOld = if (pass == 0) {
            DenseMatrix.zeros[Double](blockIdxs.size, nClasses)
          } else {
            wLocals(l)(block)
          }
          val lhs = k_bb + DenseMatrix.eye[Double](k_bb.rows) * lambdas(l)
          // Subtract out K_bb * W_bb from residual
          println(k_bb.rows)
          println(k_bb.cols)
          val rhs = y_bb - (residuals(l) - k_bb.t * wBlockOld)

          val wBlockNew = lhs \ rhs
          wLocals(l)(block) = wBlockNew
          val wBlockOldBC = labels.context.broadcast(wBlockOld)
          val wBlockNewBC = labels.context.broadcast(wBlockNew)
          (wBlockOldBC, wBlockNewBC)
        }

        val localSolveTime = timeElasped(localSolveBegin)

        val newModelBegin = System.nanoTime
        var newModel = updateModel(models, wBlockBCs.map(_._2), blockIdxsBC, preFixLengthsBC).cache()

        // This is to truncate the lineage every 50 blocks
        if (labels.context.getCheckpointDir.isDefined && block % 50 == 49) {
          newModel = MatrixUtils.truncateLineage(newModel, false)
        }

        // materialize the new model
        newModel.count()

        models.unpersist(true)
        models = newModel

        val modelUpdateTime = timeElasped(newModelBegin)

        println(s"EPOCH_${pass}_BLOCK ${block} took " + timeElasped(blockBegin) + " seconds")
        println(s"EPOCH_${pass}_BLOCK ${block} kernelGen: ${kernelTime} residual: ${residualTime} " +
                s"collect: ${collectTime} localSolve: ${localSolveTime} modelUpdate: ${modelUpdateTime}")

        // Unpersist the kernel block as we are done with it
        if (!cacheKernel) {
          kernelBlockMat.unpersist(true)
        }

        // TODO(stephentu): if I try to destroy() here, it throws an error
        // complaining we are trying to use a destroyed broadcast variable
        wBlockBCs.map { case (wBlockOldBC, wBlockNewBC) =>
          wBlockOldBC.unpersist(true)
          wBlockNewBC.unpersist(true)
        }
        blockIdxsBC.unpersist(true)
      }
    }

    /* Models for lambdas */
    val localModelSplit:Array[Array[DenseMatrix[Double]]] = models.collect()
    val localModel:Array[DenseMatrix[Double]] = (0 until lambdas.size).toArray.map(i => localModelSplit.map(_(i)).reduce(DenseMatrix.vertcat(_,_)))

    // TODO(vaishaal): Intercepts
    // TODO(vaishaal): Multi lambda
    new KernelBlockModel(localModel(0), blockSize, lambdas, kernelBlockGenerator, nTrain)
  }

  def updateModel(
      model: RDD[Array[DenseMatrix[Double]]],
      wBlockNewBC: Seq[Broadcast[DenseMatrix[Double]]],
      blockIdxsBC: Broadcast[Array[Int]],
      preFixLengthsBC: Broadcast[Array[Int]]): RDD[Array[DenseMatrix[Double]]] = {
    var newModel = model.mapPartitionsWithIndex { case (idx, part) =>

      val partBegin = preFixLengthsBC.value(idx) // note that prefix length is *not*
                                                // cumsum (so the first entry is 0)
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
        // val inds =
        //   (partBegin until (partBegin + partLength)).toSet.intersect(blockIdxsBC.value.toSet).toSeq.sorted
        // val partInds = inds.map(i => i - partBegin)
        // val blockInds = inds.map(x => blockIdxsBC.value.indexOf(x))

        val responsibleRange = (partBegin until (partBegin + partLength)).toSet
        val inds = blockIdxsBC.value.zipWithIndex.filter { case (choice, _) => responsibleRange.contains(choice) }
        val partInds = inds.map(x => x._1 - partBegin).toSeq
        val blockInds = inds.map(x => x._2).toSeq

        val wBlockNewBCvalue = wBlockNewBC(idx).value
        wPart(partInds, ::) := wBlockNewBCvalue(blockInds, ::)
      }
      Iterator.single(wParts)
    }
    newModel
  }

  def addMatrixArrays(a: Array[DenseMatrix[Double]], b: Array[DenseMatrix[Double]]) = {
    var i = 0
    while (i < a.length) {
      a(i) += b(i)
      i = i + 1
    }
    a
  }

  def timeElasped(ns: Long) : Double = (System.nanoTime - ns).toDouble / 1e9
}
