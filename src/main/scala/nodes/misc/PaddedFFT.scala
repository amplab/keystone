package nodes.misc

import breeze.linalg.DenseVector
import breeze.math.Complex
import pipelines.Transformer

/**
 * This transformer pads input vectors to the nearest power of two,
 * then returns the real values of the first half of the fourier transform on the padded vectors.
 *
 * Goes from vectors of size n to vectors of size nextPowerOfTwo(n)/2
 */
object PaddedFFT extends Transformer[DenseVector[Double], DenseVector[Double]] {
  override def apply(in: DenseVector[Double]): DenseVector[Double] = {
    val paddedSize = nextPowerOfTwo(in.length)
    val fft: DenseVector[Complex] = breeze.signal.fourierTr(in.padTo(paddedSize, 0.0).toDenseVector)
    fft(0 until (paddedSize / 2)).map(_.real)
  }

  def nextPowerOfTwo(numInputFeatures: Int) = {
    var v = numInputFeatures - 1
    v |= v >> 1
    v |= v >> 2
    v |= v >> 4
    v |= v >> 8
    v |= v >> 16
    v + 1
  }
}