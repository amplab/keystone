package nodes.stats

import breeze.linalg.{DenseVector, DenseMatrix}
import org.apache.spark.rdd.RDD
import pipelines.Transformer

/**
 * Given a collection of Dense Matrices, this will generate a sample of `numSamples` columns from the entire set.
 * @param numSamples
 */
class ColumnSampler(numSamples: Int) extends Transformer[DenseMatrix[Float], DenseVector[Float]] {
  override def apply(in: RDD[DenseMatrix[Float]]): RDD[DenseVector[Float]] = {
    val numImgs = in.count.toInt
    val samplesPerImage = numSamples/numImgs

    in.flatMap(mat => {
      (0 until samplesPerImage).map( x => {
        mat(::, scala.util.Random.nextInt(mat.cols)).toDenseVector
      })
    })
  }

  def apply(in: DenseMatrix[Float]): DenseVector[Float] = ???
}