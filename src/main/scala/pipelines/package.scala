import org.jblas.DoubleMatrix
import org.jblas.FloatMatrix

/**
 * Package object contains a number of implicits and type definitions useful for working with pipelines.
 * It is recommended that users `import pipelines._` to use them.
 */
package object pipelines {
  /**
   * Type of numeric data used by pipelines.
   */
  type DataType = Float

  /**
   * Type of numeric matrices used by pipelines. Should be consistent with `DataType`.
   */
  type MatrixType = FloatMatrix

  /**
   * Converts a double to a Float.
   * @param x A double precision floating point number.
   * @return Single precision floating point representation of the input.
   */
  implicit def double2Float(x: Double): Float = x.toFloat

  /**
   * Converts an array of Double to an array of Float. See `double2Float`.
   * @param x An array of doubles.
   * @return Array of floats.
   */
  implicit def double2FloatArr(x: Array[Double]): Array[Float] = x.map(_.toFloat)

  /**
   * Converts an two-dimensional array of Double to a two-dimensional Array of Float. See `double2Float`.
   * @param x A two-dimensional array of Double.
   * @return A two-dimensional array of Float.
   */
  implicit def double2FloatArrArr(x: Array[Array[Double]]): Array[Array[Float]] = x.map(y => y.map(_.toFloat))
}
