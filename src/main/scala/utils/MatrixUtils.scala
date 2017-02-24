package utils

import java.io.File

import scala.reflect.ClassTag
import scala.util.Random

import breeze.linalg._

import org.apache.spark.rdd.RDD

import edu.berkeley.cs.amplab.mlmatrix.util.{Utils => MLMatrixUtils}

/**
 * A collection of utilities useful for matrices.
 */
object MatrixUtils extends Serializable {

  /**
   * Converts a matrix to an array of rows.
   * @param mat Input matrix.
   * @return Array of rows.
   */
  def matrixToRowArray[T : ClassTag](mat: DenseMatrix[T]): Array[DenseVector[T]] = {
    val matT = mat.t
    // The explicit copy of the vector is necessary because otherwise Breeze slices
    // lazily, leading to inflated serialization size (A serious issue w/ spark)
    (0 until mat.rows).toArray.map(x => DenseVector(matT(::, x).toArray))
  }

  /**
   * Converts a matrix to an array of columns.
   * @param mat Input matrix.
   * @return Array of columns.
   */
  def matrixToColArray[T : ClassTag](mat: DenseMatrix[T]): Array[DenseVector[T]] = {
    // The explicit copy of the vector is necessary because otherwise Breeze slices
    // lazily, leading to inflated serialization size (A serious issue w/ spark)
    (0 until mat.cols).toArray.map(x => DenseVector(mat(::, x).toArray))
  }

  /**
   * Converts a sequence of DenseVector to a matrix where each vector is a row.
   *
   * @param in Sequence of of DenseVectors (rows)
   * @return Iterator with a single element if rows is non-empty. Empty iterator otherwise.
   */
  def rowsToMatrixIter[T: ClassTag](in: TraversableOnce[DenseVector[T]]): Iterator[DenseMatrix[T]] =
  {
    if (!in.isEmpty) {
      Iterator.single(rowsToMatrix(in))
    } else {
      Iterator.empty
    }
  }

  /**
   * Converts a sequence of DenseVector to a matrix where each vector is a row.
   *
   * @param in Sequence of of DenseVectors (rows)
   * @return A row matrix.
   */
  def rowsToMatrix[T : ClassTag](in: TraversableOnce[DenseVector[T]]): DenseMatrix[T] = {
    if (!in.isEmpty) {
      rowsToMatrix(in.toArray)
    } else {
      new DenseMatrix[T](0, 0)
    }
  }

  /**
   * Converts an array of DenseVector to a matrix where each vector is a row.
   *
   * @param inArr Array of DenseVectors (rows)
   * @return A row matrix.
   */
  def rowsToMatrix[T : ClassTag](inArr: Array[DenseVector[T]]): DenseMatrix[T] = {
    val nRows = inArr.length
    val nCols = inArr(0).length
    val outArr = new Array[T](nRows * nCols)
    var i = 0
    while (i < nRows) {
      var j = 0
      val row = inArr(i)
      while (j < nCols) {
        outArr(i + nRows * j) = row(j)
        j = j + 1
      }
      i = i + 1
    }
    val outMat = new DenseMatrix[T](nRows, nCols, outArr)
    outMat
  }

  /**
   * Converts a RDD of DenseVector to a RDD of DenseMatrix such that vectors in each partition
   * are stacked into a rows to make a DenseMatrix.
   *
   * @param in RDD of DenseVectors (row)
   * @return RDD containing dense matrices
   */
  def rowsToMatrix[T: ClassTag] (in: RDD[DenseVector[T]]) : RDD[DenseMatrix[T]]= {
    in.mapPartitions { part =>
      rowsToMatrixIter(part)
    }
  }


  /**
   * Draw samples rows from a matrix.
   *
   * @param in Input matrix.
   * @param numSamples Number of samples to draw.
   * @return A matrix constructed from a sample of the rows.
   */
  def sampleRows(in: DenseMatrix[Double], numSamples: Int): DenseMatrix[Double] = {
    val rows = Random.shuffle(0 to (in.rows-1)).take(numSamples).sorted
    (in(rows,::)).toDenseMatrix
  }

  // In place deterministic shuffle
  def shuffleArray[T](arr: Array[T], seed: Int = 42) = {
    // Shuffle each row in the same fashion
    val rnd = new java.util.Random(seed)
    var i = arr.length - 1
    while (i > 0) {
      val index = rnd.nextInt(i + 1)
      // Simple swap
      val a = arr(index)
      arr(index) = arr(i)
      arr(i) = a
      i = i - 1
    }
    arr
  }

  def computeMean(in: RDD[DenseMatrix[Double]]): DenseVector[Double] = {
    val sumCount = MLMatrixUtils.treeReduce(in.map { mat =>
      (sum(mat(::, *)).t, mat.rows)
    }, (a: (DenseVector[Double], Int), b: (DenseVector[Double], Int)) => {
      a._1 += b._1
      (a._1, a._2 + b._2)
    })

    sumCount._1 /= sumCount._2.toDouble
  }

  /**
   * Check if a given sequence is sorted.
   *
   * @param s Sequence that needs to be checked.
   * @return true if the squence is sorted. false otherwise
   */
  def isSorted[T](s: Seq[T])(implicit cmp: Ordering[T]): Boolean = {
    if (s.isEmpty) {
      true 
    } else {
      var i = 1
      while (i < s.size) {
        if (cmp.gt(s(i - 1), s(i)))
          return false
        i += 1
      }
      true
    }
  }

  // Truncates the lineage of an RDD and returns a new RDD
  // that is in memory and has truncated lineage
  def truncateLineage[T: ClassTag](in: RDD[T], cache: Boolean): RDD[T] = {
    // What we are doing here is:
    // cache the input before checkpoint as it triggers a job
    if (cache) {
      in.cache()
    }
    in.checkpoint()
    // Run a count to trigger the checkpoint
    in.count

    // Now "in" has HDFS preferred locations which is bothersome
    // when we zip it next time. So do a identity map & get an RDD
    // that is in memory, but has no preferred locs
    val out = in.map(x => x).cache()
    // This stage will run as NODE_LOCAL ?
    out.count

    // Now out is in memory, we can get rid of "in" and then
    // return out
    if (cache) {
      in.unpersist(true)
    }
    out
  }

  // Add two sequence of matrices by updating the first argument in place
  def addMatrices(a: IndexedSeq[DenseMatrix[Double]], b: IndexedSeq[DenseMatrix[Double]]) = {
    var i = 0
    while (i < a.length) {
      a(i) += b(i)
      i = i + 1
    }
    a
  }

}
