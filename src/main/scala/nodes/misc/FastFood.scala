package nodes.misc

import breeze.linalg.DenseVector
import breeze.math.Complex
import pipelines.Transformer


class FastFood(numFeatures: Int) extends Transformer[DenseVector[Double], DenseVector[Double]] {
  val paddedSize = nextPowerOfTwo(numFeatures)
  override def apply(in: DenseVector[Double]): DenseVector[Double] = {
    val fft: DenseVector[Complex] = breeze.signal.fourierTr(in.padTo(paddedSize, 0.0).toDenseVector)
    fft(0 until (paddedSize / 2)).map(_.real)
  }

  def nextPowerOfTwo(numInputFeatures: Int) = {
    math.pow(2, math.ceil(math.log(numInputFeatures)/math.log(2))).toInt
  }
}