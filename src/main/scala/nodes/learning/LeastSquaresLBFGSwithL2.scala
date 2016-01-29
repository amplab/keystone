package nodes.learning

import breeze.linalg.Vector

/**
 * Solve Least Squares with L2 loss using Sparse LBFGS
 *
 * @param fitIntercept Whether to fit the intercepts or not.
 * @param numCorrections 3 < numCorrections < 10 is recommended.
 * @param convergenceTol convergence tolerance for L-BFGS
 * @param numIterations max number of iterations to run
 * @param regParam L2 regularization
 * @param sparseOverhead The overhead of sparse gradient over dense gradient (used for cost modeling)
 * @tparam T
 */
case class LeastSquaresSparseLBFGSwithL2[T <: Vector[Double]](
    override val fitIntercept: Boolean = true,
    override val numCorrections: Int = 10,
    override val convergenceTol: Double = 1e-4,
    override val numIterations: Int = 100,
    override val regParam: Double = 0.0,
    sparseOverhead: Double = 8)
  extends SparseLBFGSwithL2[T](new LeastSquaresSparseGradient) with SolverWithCostModel[T] {

  override def cost(
    n: Long,
    d: Int,
    k: Int,
    sparsity: Double,
    numMachines: Int,
    cpuWeight: Double,
    memWeight: Double,
    networkWeight: Double)
  : Double = {
    val flops =  n.toDouble * sparsity * d * k / numMachines // Time to compute a sparse gradient.
    val bytesScanned = n.toDouble * d * sparsity
    val network = 2.0 * d * k * math.log(numMachines) // Need to communicate the dense model. Treereduce

    numIterations *
      (sparseOverhead * math.max(cpuWeight * flops, memWeight * bytesScanned) + networkWeight * network)
  }
}

/**
 * Solve Least Squares with L2 loss using Dense LBFGS
 *
 * @param fitIntercept Whether to fit the intercepts or not.
 * @param numCorrections 3 < numCorrections < 10 is recommended.
 * @param convergenceTol convergence tolerance for L-BFGS
 * @param numIterations max number of iterations to run
 * @param regParam L2 regularization
 * @tparam T
 */
case class LeastSquaresDenseLBFGSwithL2[T <: Vector[Double]](
    override val fitIntercept: Boolean = true,
    override val numCorrections: Int = 10,
    override val convergenceTol: Double = 1e-4,
    override val numIterations: Int = 100,
    override val regParam: Double = 0.0)
  extends DenseLBFGSwithL2[T](new LeastSquaresDenseGradient) with SolverWithCostModel[T] {

  override def cost(
    n: Long,
    d: Int,
    k: Int,
    sparsity: Double,
    numMachines: Int,
    cpuWeight: Double,
    memWeight: Double,
    networkWeight: Double)
  : Double = {
    val flops =  n.toDouble * sparsity * d * k / numMachines // Time to compute a sparse gradient.
    val bytesScanned = n.toDouble * d * sparsity
    val network = 2.0 * d * k * math.log(numMachines) // Need to communicate the dense model. Treereduce

    numIterations *
      (math.max(cpuWeight * flops, memWeight * bytesScanned) + networkWeight * network)
  }
}
