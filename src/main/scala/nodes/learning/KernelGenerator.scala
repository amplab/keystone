package nodes.learning

import breeze.linalg._
import breeze.math._
import breeze.numerics._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import utils._


// Trait to be implemented by Kernel functions
trait KernelGenerator[T] {

  // TODO(stephentu): these should both be just one function

  /**
   * Return a column block of the kernel train matrix
   *
   * The kernel train matrix is a |train| x |train| symmetric matrix K_train.
   * This function returns the columns in blockIdxs of K_train  and the block diagonal.
   */
  def generateKernelTrainBlock(mat: RDD[T], blockIdxs: Seq[Int]):
      (RDD[DenseVector[Double]], DenseMatrix[Double])

  /**
   * Return a column block of the kernel test matrix
   *
   * The kernel test matrix is a |test| x |train| matrix K_testtrain.
   * This function returns the columns in blockIdxs of K_testtrain.
   *
   * WARNING: Observe the order that test comes first
   */
  def generateKernelTestBlock(
    testMat: RDD[T],
    trainMat: RDD[T],
    blockIdxs: Seq[Int]): RDD[DenseVector[Double]]

}

object SparseLinearKernel extends KernelGenerator[(Array[Long], Array[Double])]
  with Serializable {

  private def sparseDotProduct(
    lhsInds: Array[Long], lhsValues: Array[Double],
    rhsInds: Array[Long], rhsValues: Array[Double]): Double = {

    var s = 0.0
    var lhsIdx = 0
    var rhsIdx = 0
    while (lhsIdx < lhsInds.size && rhsIdx < rhsInds.size) {
      if (lhsInds(lhsIdx) == rhsInds(rhsIdx)) {
        s += lhsValues(lhsIdx) * rhsValues(rhsIdx)
        lhsIdx += 1
        rhsIdx += 1
      } else if (lhsInds(lhsIdx) < rhsInds(rhsIdx)) {
        lhsIdx += 1
      } else {
        rhsIdx += 1
      }
    }
    s

  }


  def generateKernelTestBlock(
    lhsMat: RDD[(Array[Long], Array[Double])],
    rhsMat: RDD[(Array[Long], Array[Double])],
    blockIdxs: Seq[Int]): RDD[DenseVector[Double]] = {

    val blockDataArray = rhsMat.zipWithIndex.filter { case (_, idx) =>
      blockIdxs.contains(idx)
    }.map(x => x._1).collect()
    val blockDataBC = rhsMat.context.broadcast(blockDataArray)
    val kBlock = lhsMat.map { case (lhsInds, lhsValues) =>
      new DenseVector(blockDataBC.value.map { case (rhsInds, rhsValues) =>
        sparseDotProduct(lhsInds, lhsValues, rhsInds, rhsValues)
      }.toArray)
    }
    kBlock.cache()
    kBlock.count
    blockDataBC.unpersist(true)
    kBlock
  }

  def generateKernelTrainBlock(
    mat: RDD[(Array[Long], Array[Double])],
    blockIdxs: Seq[Int]): (RDD[DenseVector[Double]], DenseMatrix[Double]) = {

    mat.foreach { x => assert(KernelUtils.isSorted(x._1)) } // limitation for now

    val lhsMat = mat
    val rhsMat = mat

    val blockDataArray = rhsMat.zipWithIndex.filter { case (_, idx) =>
      blockIdxs.contains(idx)
    }.map(x => x._1).collect()
    val blockDataBC = rhsMat.context.broadcast(blockDataArray)
    val Kblock = lhsMat.map { case (lhsInds, lhsValues) =>
      new DenseVector(blockDataBC.value.map { case (rhsInds, rhsValues) =>
        sparseDotProduct(lhsInds, lhsValues, rhsInds, rhsValues)
      }.toArray)
    }

    val Kblockblock = blockDataArray.par.map { case (lhsInds, lhsValues) =>
      new DenseVector(blockDataArray.map { case (rhsInds, rhsValues) =>
        sparseDotProduct(lhsInds, lhsValues, rhsInds, rhsValues)
      }.toArray)
    }.toArray

    // val Kblockblock = blockDataArray.map { case (lhsInds, lhsValues) =>
    //   new DenseVector(blockDataArray.map { case (rhsInds, rhsValues) =>
    //     sparseDotProduct(lhsInds, lhsValues, rhsInds, rhsValues)
    //   }.toArray)
    // }

    Kblock.cache()
    Kblock.count
    blockDataBC.unpersist(true)

    (Kblock, KernelUtils.rowsToMatrix(Kblockblock))
  }

}

class GaussianKernelGenerator(gamma: Double)
  extends KernelGenerator[DenseVector[Double]]
  with Serializable {

  def generateKernelTestBlock(
    testMat: RDD[DenseVector[Double]],
    trainMat: RDD[DenseVector[Double]],
    blockIdxs: Seq[Int]): RDD[DenseVector[Double]]= {

    println("testMat.count=" + testMat.count +
      ",trainMat.count=" + trainMat.count +
      ", blockSize=" + blockIdxs.length)

    // Dot product of rows of X with each other
    val trainDotProd = trainMat.map {x =>
      (x.t*x).toDouble
    }
    val testDotProd = testMat.map {x =>
      (x.t*x).toDouble
    }

    // b x d block of training data
    val blockDataArray = trainMat.zipWithIndex.filter{ case (vec, idx) =>
      blockIdxs.contains(idx)
    }.map(x=> x._1).collect()
    val blockData = KernelUtils.rowsToMatrix(blockDataArray)
    assert(blockData.rows == blockIdxs.length)
    val blockDataBC = trainMat.context.broadcast(blockData)

    // <xi,xj> for i in [nTest], j in blockIdxs
    val blockXXT = testMat.mapPartitions { itr  =>
      val bd = blockDataBC.value
      val vecMat = KernelUtils.rowsToMatrix(itr)
      Iterator.single(vecMat*bd.t)
    }
    val trainBlockDotProd = DenseVector(trainDotProd.zipWithIndex.filter{ case (vec, idx) =>
      blockIdxs.contains(idx)
    }.map(x => x._1).collect())
    val trainBlockDotProdBC = trainMat.context.broadcast(trainBlockDotProd)

    //R_block = -2.0 * xxt_term + ...
    //  mat_dot_prod_test * ones(1, length(trainblock)) + ...
    //  ones(nTest, 1) * mat_dot_prod_train(trainblock)';
    val kTestBlock = blockXXT.zipPartitions(testDotProd) { case (iterXXT, iterTestDotProds) =>
      val xxt = iterXXT.next()
      assert(iterXXT.isEmpty)
      iterTestDotProds.zipWithIndex.map { case (testDotProd, idx) =>
        val term1 = xxt(idx, ::).t * (-2.0)
        val term2 = DenseVector.fill(xxt.cols){testDotProd}
        val term3 = trainBlockDotProdBC.value
        val term4 = (term1 + term2 + term3) * (-gamma)
        exp(term4)
      }
    }

    kTestBlock.cache()
    kTestBlock.count
    blockDataBC.unpersist()
    trainBlockDotProdBC.unpersist()
    kTestBlock
  }

  // Generate a block of size n x b of the kernel matrix
  def generateKernelTrainBlock(
    mat: RDD[DenseVector[Double]],
    blockIdxs: Seq[Int]): (RDD[DenseVector[Double]], DenseMatrix[Double]) = {

    assert(KernelUtils.isSorted(blockIdxs)) // limitation for now

    println("mat.count=" + mat.count + ", blockSize=" + blockIdxs.length)

    // Dot product of rows of X with each other
    val matDotProd = mat.map {x =>
      (x.t*x).toDouble
    }

    // Cache the indexes as we will use it twice
    val matIdxs = mat.zipWithIndex.map(_._2).cache()
    matIdxs.count

    // block_data = mat(block,:)
    // b x d
    //TODO: Combine the collect of the data array with the collect of the
    //matDotProd below so that only one job is triggered
    val blockDataArray = mat.zip(matIdxs).filter{ case (vec, idx) =>
      blockIdxs.contains(idx)
    }.map(x => x._1).collect()

    val blockData = KernelUtils.rowsToMatrix(blockDataArray)
    
    val blockDataBC = mat.context.broadcast(blockData)

    val blockMatDotProd = DenseVector(matDotProd.zip(matIdxs).filter{ case (vec, idx) =>
      blockIdxs.contains(idx)
    }.map(x => x._1).collect())

    println("DEBUG blockMatDotProd.length=" + blockMatDotProd.length)
    val blockMatDotProdBC = mat.context.broadcast(blockMatDotProd)

    matIdxs.unpersist()

    // <xi,xj> for i in [nTrain], j in blockIdxs
    val kBlock = mat.zipPartitions(matDotProd) { case (iterMat, iterDotProds) =>
      val bd = blockDataBC.value
      val bmdp = blockMatDotProdBC.value

      val xMat = KernelUtils.rowsToMatrix(iterMat.toArray)
      val vecDotProds = DenseVector(iterDotProds.toArray)
      val XXTTerm = xMat * bd.t

      //// non-vectorized version
      //iterDotProds.zipWithIndex.map { case (dotProd, idx) =>
      //  val term1 = XXTTerm(idx, ::).t * (-2.0)
      //  val term2 = DenseVector.fill(XXTTerm.cols){dotProd}
      //  val term3 = blockMatDotProdBC.value
      //  val term4 = (term1 + term2 + term3) * (-gamma)
      //  exp(term4)
      //}

      //// vectorized version
      //val vecDotProds = DenseVector(iterDotProds.toArray)
      //val columnRep = DenseMatrix.zeros[Double](vecDotProds.length, XXTTerm.cols)
      //columnRep(::, *) := vecDotProds
      //val rowRep = DenseMatrix.zeros[Double](vecDotProds.length, XXTTerm.cols)
      //rowRep(*, ::) := blockMatDotProdBC.value
      //KernelUtils.matrixToRowArray(exp((XXTTerm * (-2.0) + columnRep + rowRep) * (-gamma))).toIterator

      // in place vectorized version
      XXTTerm :*= (-2.0)
      XXTTerm(::, *) :+= vecDotProds
      XXTTerm(*, ::) :+= bmdp
      KernelUtils.matrixToRowArray(exp(XXTTerm * (-gamma))).iterator
    }
    kBlock.cache()
    kBlock.count
    blockMatDotProdBC.unpersist(true)
    blockDataBC.unpersist(true)

    val kBlockBlock = blockData * blockData.t
    kBlockBlock :*= (-2.0)
    kBlockBlock(::, *) :+= blockMatDotProd
    kBlockBlock(*, ::) :+= blockMatDotProd

    (kBlock, exp(kBlockBlock * (-gamma)))
  }
}
