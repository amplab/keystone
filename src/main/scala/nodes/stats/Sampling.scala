package nodes.stats

import breeze.linalg.{DenseVector, DenseMatrix}
import org.apache.spark.rdd.RDD
import pipelines.FunctionNode
import workflow.Transformer

/**
 * Given a collection of Dense Matrices, this will generate a sample of
 * @param numSamplesPerMatrix columns from each matrix.
 */
case class ColumnSampler(numSamplesPerMatrix: Int)
  extends Transformer[DenseMatrix[Float], DenseMatrix[Float]] {

  def apply(in: DenseMatrix[Float]): DenseMatrix[Float] = {
    val cols = Seq.fill(numSamplesPerMatrix) {
      scala.util.Random.nextInt(in.cols)
    }
    in(::, cols).toDenseMatrix
  }
}


/**
 * Takes a sample of an input RDD of size size.
 * @param size Number of elements to return.
 */
class Sampler[T](val size: Int, val seed: Int = 42) extends FunctionNode[RDD[T], Array[T]] {
  def apply(in: RDD[T]): Array[T] = {
    in.takeSample(false, size, seed)
  }
}
