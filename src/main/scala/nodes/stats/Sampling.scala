package nodes.stats

import breeze.linalg.{DenseVector, DenseMatrix}
import org.apache.spark.rdd.RDD
import pipelines.{FunctionNode, Transformer}

/**
 * Given a collection of Dense Matrices, this will generate a sample of `numSamples` columns from the entire set.
 * @param numSamples
 */
class ColumnSampler(
    numSamples: Int,
    numImgsOpt: Option[Int] = None) extends Transformer[DenseMatrix[Float], DenseVector[Float]] {
  override def apply(in: RDD[DenseMatrix[Float]]): RDD[DenseVector[Float]] = {
    val numImgs = numImgsOpt.getOrElse(in.count.toInt)
    val samplesPerImage = numSamples/numImgs

    in.flatMap(mat => {
      (0 until samplesPerImage).map( x => {
        mat(::, scala.util.Random.nextInt(mat.cols)).toDenseVector
      })
    })
  }

  def apply(in: DenseMatrix[Float]): DenseVector[Float] = ???
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
