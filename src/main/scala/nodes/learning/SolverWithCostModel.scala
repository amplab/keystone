package nodes.learning

import breeze.linalg._
import workflow.LabelEstimator

/**
 * A trait that represents a model solver with a known system performance cost model.
 */
trait SolverWithCostModel[T <: Vector[Double]]
  extends LabelEstimator[T, DenseVector[Double], DenseVector[Double]] {

  def cost(
    n: Long,
    d: Int,
    k: Int,
    sparsity: Double,
    numMachines: Int,
    cpuWeight: Double,
    memWeight: Double,
    networkWeight: Double)
  : Double
}