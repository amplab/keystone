package nodes.misc

import breeze.linalg.DenseVector
import breeze.math.Complex
import pipelines.Transformer

/**
 * This transformer pads input vectors to the nearest power of two,
 * then returns the real values of the first half of the fourier transform on the padded vectors.
 *
 * Goes from vectors of size n to vectors of size nextPowerOfTwo(n)/2
 * @param numFeatures The size of the input vectors
 */
class PaddedFFT(numFeatures: Int) extends Transformer[DenseVector[Double], DenseVector[Double]] {
  val paddedSize = nextPowerOfTwo(numFeatures)
  override def apply(in: DenseVector[Double]): DenseVector[Double] = {
    val fft: DenseVector[Complex] = breeze.signal.fourierTr(in.padTo(paddedSize, 0.0).toDenseVector)
    fft(0 until (paddedSize / 2)).map(_.real)
  }

  def nextPowerOfTwo(numInputFeatures: Int) = {
    math.pow(2, math.ceil(math.log(numInputFeatures)/math.log(2))).toInt
  }
}