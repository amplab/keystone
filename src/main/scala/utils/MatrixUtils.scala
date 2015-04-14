package utils

import java.io.File
import breeze.linalg._

import scala.reflect.ClassTag


/**
 * A collection of utilities useful for matrices.
 */
object MatrixUtils extends Serializable {
  /**
   * Read a CSV file from disk.
   *
   * @param filename Local file name.
   * @return A matrix of numbers contained in the file.
   */
  def loadCSVFile(filename: String): DenseMatrix[Double] = {
    csvread(new File(filename))
  }

  /**
   * Write a CSV file to disk.
   *
   * @param filename Local filename.
   * @param matrix Matrix to write.
   */
  def writeCSVFile(filename: String, matrix: DenseMatrix[Double]) = {
    csvwrite(new File(filename), matrix)
  }

  /**
   * Converts a matrix to an array of row arrays.
   * @param mat Input matrix.
   * @return Array of rows.
   */
  def matrixToRowArray[T](mat: DenseMatrix[T])(implicit ct: ClassTag[T]): Array[Array[T]] = {
    val matT = mat.t
    (0 until mat.rows).toArray.map(matT(::, _).toArray)
  }

}