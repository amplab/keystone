package nodes.learning

import breeze.linalg.{SparseVector, DenseMatrix, DenseVector}
import breeze.stats.distributions.Rand
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import pipelines._
import utils.Stats
import workflow.WorkflowUtils

import scala.collection.mutable.ArrayBuffer

class OptimizableLeastSquaresSolverSuite extends FunSuite with LocalSparkContext with Logging {

  test("Big n small d dense") {
    sc = new SparkContext("local", "test")

    val n = 1000000
    val sampleRatio = 0.001
    val d = 1000
    val k = 1000
    val numMachines = 16

    val data = sc.parallelize(Seq.fill((n * sampleRatio).toInt)(DenseVector.rand[Double](d)))
    val labels = data.map(_ => DenseVector.rand[Double](k))
    val numPerPartition = WorkflowUtils.numPerPartition(data).mapValues(x => (x / sampleRatio).toInt)

    val solver = new OptimizableLeastSquaresSolver[DenseVector[Double]](numMachines = Some(numMachines))
    val optimizedSolver = solver.optimize(data, labels, numPerPartition)

    assert(optimizedSolver.isInstanceOf[LinearMapEstimator[_]], "Expected exact distributed solver")
  }

  test("big n big d dense") {
    sc = new SparkContext("local", "test")

    val n = 1000000
    val sampleRatio = 0.0001
    val d = 10000
    val k = 1000
    val numMachines = 16

    val data = sc.parallelize(Seq.fill((n * sampleRatio).toInt)(DenseVector.rand[Double](d)))
    val labels = data.map(_ => DenseVector.rand[Double](k))
    val numPerPartition = WorkflowUtils.numPerPartition(data).mapValues(x => (x / sampleRatio).toInt)

    val solver = new OptimizableLeastSquaresSolver[DenseVector[Double]](numMachines = Some(numMachines))
    val optimizedSolver = solver.optimize(data, labels, numPerPartition)

    assert(optimizedSolver.isInstanceOf[BlockLeastSquaresEstimator[_]], "Expected block solver")
  }

  test("big n big d sparse") {
    sc = new SparkContext("local", "test")

    val n = 1000000
    val sampleRatio = 0.0001
    val d = 10000
    val k = 2
    val sparsity = 0.01
    val numMachines = 16

    val data = sc.parallelize(Seq.fill((n * sampleRatio).toInt) {
      val sparseVec = SparseVector.zeros[Double](d)
      DenseVector.rand[Double]((sparsity * d).toInt).toArray.zipWithIndex.foreach {
        case (value, i) =>
          sparseVec(i) = value
      }
      sparseVec
    })
    val labels = data.map(_ => DenseVector.rand[Double](k))
    val numPerPartition = WorkflowUtils.numPerPartition(data).mapValues(x => (x / sampleRatio).toInt)

    val solver = new OptimizableLeastSquaresSolver[SparseVector[Double]](numMachines = Some(numMachines))
    val optimizedSolver = solver.optimize(data, labels, numPerPartition)

    assert(optimizedSolver.isInstanceOf[LeastSquaresSparseLBFGSwithL2[_]], "Expected block solver")
  }
}
