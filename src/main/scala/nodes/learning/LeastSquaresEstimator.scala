package nodes.learning

import breeze.linalg._
import nodes.util.{Densify, Sparsify}
import org.apache.spark.rdd.RDD
import pipelines.Logging
import workflow._

import scala.reflect._

/**
  * A least squares solver that is optimized to use a fast algorithm, based on characteristics of the
  * workload and cost models.
  *
  * Currently selects between Dense LBFGS, Sparse LBFGS, Exact Distributed Solve, and Approximate Block Solve.
  *
  * The default weights were determined empirically via results run on a 16 r3.4xlarge node cluster.
  *
  * @param lambda  The L2 regularization parameter to use, defaults to 0
  * @param numMachines
  * @param cpuWeight
  * @param memWeight
  * @param networkWeight
  * @tparam T
  */
class LeastSquaresEstimator[T <: Vector[Double]: ClassTag](
    lambda: Double = 0,
    numMachines: Option[Int] = None,
    cpuWeight: Double = 3.8e-4,
    memWeight: Double = 2.9e-1,
    networkWeight: Double = 1.32)
  extends OptimizableLabelEstimator[T, DenseVector[Double], DenseVector[Double]]
    with WeightedNode
    with Logging {

  val options: Seq[(CostModel, LabelEstimator[T, DenseVector[Double], DenseVector[Double]])] = Seq(
    {
      val solver = new DenseLBFGSwithL2[T](new LeastSquaresDenseGradient, regParam = lambda, numIterations = 20)
      (solver, solver)
    },
    {
      val solver = new SparseLBFGSwithL2(new LeastSquaresSparseGradient, regParam = lambda, numIterations = 20)
      (solver, TransformerLabelEstimatorChain(Sparsify(), solver))
    },
    {
      val solver = new BlockLeastSquaresEstimator(1000, 3, lambda = lambda)
      (solver, TransformerLabelEstimatorChain(Densify(), solver))
    },
    {
      val solver = new LinearMapEstimator(Some(lambda))
      (solver, TransformerLabelEstimatorChain(Densify(), solver))
    }
  )

  override val default: LabelEstimator[T, DenseVector[Double], DenseVector[Double]] with WeightedNode = {
    new DenseLBFGSwithL2[T](new LeastSquaresDenseGradient, regParam = lambda, numIterations = 20)
  }

  override def optimize(
      sample: RDD[T],
      sampleLabels: RDD[DenseVector[Double]],
      numPerPartition: Map[Int, Int])
  : LabelEstimator[T, DenseVector[Double], DenseVector[Double]] = {
    val n = numPerPartition.values.map(_.toLong).sum
    val d = sample.first().length
    val k = sampleLabels.first().length
    val sparsity = sample.map(x => x.activeSize.toDouble / x.length).sum() / sample.count()

    val realNumMachines = numMachines.getOrElse {
      if (sample.sparkContext.getExecutorStorageStatus.length == 1) {
        1
      } else {
        sample.sparkContext.getExecutorStorageStatus.length - 1
      }
    }

    logDebug(s"Optimizable Param n is $n")
    logDebug(s"Optimizable Param d is $d")
    logDebug(s"Optimizable Param k is $k")
    logDebug(s"Optimizable Param sparsity is $sparsity")
    logDebug(s"Optimizable Param numMachines is $realNumMachines")

    options.minBy(_._1.cost(n, d, k, sparsity, realNumMachines, cpuWeight, memWeight, networkWeight))._2
  }

  override val weight: Int = default.weight
}