package keystoneml.nodes.learning

import breeze.linalg._
import org.apache.spark.rdd.RDD
import keystoneml.workflow.Transformer

/**
 * Computes A * x + b i.e. a linear map of data using a trained model.
 *
 * @param x trained model
 * @param bOpt optional intercept to add
 */
case class SparseLinearMapper(
    x: DenseMatrix[Double],
    bOpt: Option[DenseVector[Double]] = None)
  extends Transformer[SparseVector[Double], DenseVector[Double]] {

  /**
   * Apply a linear model to an input.
   * @param in Input.
   * @return Output.
   */
  def apply(in: SparseVector[Double]): DenseVector[Double] = {
    val out = x.t * in
    bOpt.foreach { b =>
      out :+= b
    }

    out
  }

  /**
   * Apply a linear model to a collection of inputs.
   *
   * @param in Collection of A's.
   * @return Collection of B's.
   */
  override def apply(in: RDD[SparseVector[Double]]): RDD[DenseVector[Double]] = {
    val modelBroadcast = in.context.broadcast(x)
    val bBroadcast = in.context.broadcast(bOpt)
    in.map(row => {
      val out = modelBroadcast.value.t * row
      bBroadcast.value.foreach { b =>
        out :+= b
      }

      out
    })
  }
}
