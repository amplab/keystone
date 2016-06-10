package nodes.learning

import breeze.linalg.{DenseVector, SparseVector}
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines._
import workflow.{TransformerLabelEstimatorChain, PipelineContext, WorkflowUtils}

class LeastSquaresEstimatorSuite extends FunSuite with PipelineContext with Logging {

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

    val solver = new LeastSquaresEstimator[DenseVector[Double]](numMachines = Some(numMachines))
    val optimizedSolver = solver.optimize(data, labels, numPerPartition)

    val isLinearMapEstimator = optimizedSolver match {
      case t: TransformerLabelEstimatorChain[_,_,_,_] => {
        t.second match {
          case _: LinearMapEstimator => true
          case _ => false
        }
      }
      case _ => false
    }
    assert(isLinearMapEstimator, "Expected exact distributed solver")
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

    val solver = new LeastSquaresEstimator[DenseVector[Double]](numMachines = Some(numMachines))
    val optimizedSolver = solver.optimize(data, labels, numPerPartition)

    val isBlockSolver = optimizedSolver match {
      case t: TransformerLabelEstimatorChain[_,_,_,_] => {
        t.second match {
          case _: BlockLeastSquaresEstimator => true
          case _ => false
        }
      }
      case _ => false
    }

    assert(isBlockSolver, "Expected block solver")
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

    val solver = new LeastSquaresEstimator[SparseVector[Double]](numMachines = Some(numMachines))
    val optimizedSolver = solver.optimize(data, labels, numPerPartition)

    val isSparseLBFGS = optimizedSolver match {
      case t: TransformerLabelEstimatorChain[_,_,_,_] => {
        t.second match {
          case _: SparseLBFGSwithL2 => true
          case _ => false
        }
      }
      case _ => false
    }

    assert(isSparseLBFGS, "Expected sparse LBFGS solver")
  }
}