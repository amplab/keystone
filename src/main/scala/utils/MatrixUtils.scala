package utils

import java.io.File

import breeze.linalg._

import scala.reflect.ClassTag
import scala.util.Random

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
   *  Converts a RDD of DenseVector to a RDD of DenseMatrix such that each partition
   *  of  DenseVectors are stacked into a rows to make  DenseMatrix
   */

  def rowsToMatrix[T: ClassTag] (in: RDD[DenseVector[T]]) : RDD[DenseMatrix[T]]= {
    in.mapPartitions { part =>
      Iterator.single(rowsToMatrix(part))
    }
  }
  /**
   * Converts a  RDD of DenseMatrix where each matrix is in one partition into 
   * an RDD of DenseVectors separating the matrix row wise.  Undefined behavior
   * if used on RDD of  DenseMatrix not generated  from above function
   */

  def matrixToRows [T: ClassTag] (in: RDD[DenseMatrix[T]]) : RDD[DenseVector[T]]= {
    in.mapPartitions { part =>
      val matrix = part.next()
      val nRows = matrix.rows
      val outArr = new Array[DenseVector[T]](nRows)
      var i = 0
      while (i < nRows)  {
        outArr(i) = matrix(i,::).t
        i += 1
      }
      outArr.iterator
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
      (sum(mat(::, *)).toDenseVector, mat.rows)
    }, (a: (DenseVector[Double], Int), b: (DenseVector[Double], Int)) => {
      a._1 += b._1
      (a._1, a._2 + b._2)
    })

    sumCount._1 /= sumCount._2.toDouble
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

  def isSorted[T](s: Seq[T])(implicit cmp: Ordering[T]): Boolean = {
    if (s.isEmpty) {
      true 
    } else {
      // a google search reveals that the most idiomatic way to do this scala
      // involves scalaz, links to a paper in the J. of Functional Programming,
      // and is nearly incomprehensible...
      //
      // I think I'll take a while loop and some mutable state. You can go
      // ahead and feel smug about your monads and semigroups while your code
      // is running 10x slower :)

      var i = 1
      while (i < s.size) {
        if (cmp.gt(s(i - 1), s(i)))
          return false
        i += 1
      }
      true
    }
  }
}
