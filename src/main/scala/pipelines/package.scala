import org.jblas.DoubleMatrix
import org.jblas.FloatMatrix

package object pipelines {
  type DataType = Float
  type MatrixType = FloatMatrix

  implicit def double2Float(x: Double): Float = x.toFloat
  implicit def double2FloatArr(x: Array[Double]): Array[Float] = x.map(_.toFloat)
  implicit def double2FloatArrArr(x: Array[Array[Double]]): Array[Array[Float]] = x.map(y => y.map(_.toFloat))
}
