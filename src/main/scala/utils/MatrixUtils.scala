package utils

import java.io.File

import breeze.linalg._

import scala.util.Random


/**
 * A collection of utilities useful for matrices.
 */
object MatrixUtils extends Serializable {

  /**
   * Converts a matrix to an array of row arrays.
   * @param mat Input matrix.
   * @return Array of rows.
   */
  def matrixToRowArray(mat: DenseMatrix[Double]): Array[DenseVector[Double]] = {
    val matT = mat.t
    (0 until mat.rows).toArray.map(matT(::, _))
  }

  /**
   * Converts a sequence of DenseVector to a matrix where each vector is a row.
   *
   * @param in Sequence of of DenseVectors (rows)
   * @return A row matrix.
   */
  def rowsToMatrix(in: TraversableOnce[DenseVector[Double]]): DenseMatrix[Double] = {
    rowsToMatrix(in.toArray)
  }

  /**
   * Converts an array of DenseVector to a matrix where each vector is a row.
   *
   * @param inArr Array of DenseVectors (rows)
   * @return A row matrix.
   */
  def rowsToMatrix(inArr: Array[DenseVector[Double]]): DenseMatrix[Double] = {
    val nRows = inArr.length
    val nCols = inArr(0).length
    val outArr = new Array[Double](nRows * nCols)
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
    val outMat = new DenseMatrix[Double](nRows, nCols, outArr)
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
  def shuffleArray[T](arr: Array[T]) = {
    // Shuffle each row in the same fashion
    val rnd = new java.util.Random(42)
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

}
