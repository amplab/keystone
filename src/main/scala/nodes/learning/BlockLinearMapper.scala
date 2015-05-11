package nodes.learning

import breeze.linalg.{DenseVector, DenseMatrix}
import edu.berkeley.cs.amplab.mlmatrix.{RowPartition, NormalEquations, BlockCoordinateDescent, RowPartitionedMatrix}
import org.apache.spark.rdd.RDD
import pipelines.Transformer
import utils.{MatrixUtils, Stats}


/**
 * Transformer that applies a linear model to an input.
 * Different from [[LinearMapper]] in that the matrix representing the transformation
 * is vertically split into a seq, and the vectors being transformed are likewise expected to have
 * been split into a Seq, matching the split of the transformation matrix.
 * @param xs  The chunks of the matrix representing the linear model
 */
class BlockLinearMapper(val xs: Seq[DenseMatrix[Double]])
    extends Transformer[Seq[DenseVector[Double]], DenseVector[Double]] {

  /**
   * Applies the linear model to feature vectors large enough to have been split into several RDDs.
   * @param ins  RDDs where the RDD at index i is expected to contain chunk i of all input vectors.
   *             Must all be zippable.
   * @return the output vectors
   */
  def apply(ins: TraversableOnce[RDD[DenseVector[Double]]]): RDD[DenseVector[Double]] = {
    val res = ins.toIterator.zip(xs.iterator).map {
      case (in, x) => {
        val modelBroadcast = in.context.broadcast(x)
        in.mapPartitions(rows => {
          if (!rows.isEmpty) {
            Iterator(MatrixUtils.rowsToMatrix(rows) * modelBroadcast.value)
          } else {
            Iterator.empty
          }
        })
      }
    }

    val matOut = res.reduceLeft((sum, next) => sum.zip(next).map(c => c._1 + c._2))

    matOut.flatMap(MatrixUtils.matrixToRowArray)
  }

  override def apply(ins: Seq[DenseVector[Double]]): DenseVector[Double] = {
    val res = ins.zip(xs).map {
      case (in, x) => {
        x.t * in
      }
    }

    res.reduceLeft((sum, next) => sum + next)
  }

  /**
   * Applies the linear model to feature vectors large enough to have been split into several RDDs.
   * After processing chunk i of every vector, applies @param evaluator to the intermediate output
   * vector.
   * @param ins  RDDs where the RDD at index i is expected to contain chunk i of all input vectors.
   *             Must all be zippable.
   *
   */
  def applyAndEvaluate(ins: TraversableOnce[RDD[DenseVector[Double]]], evaluator: (RDD[DenseVector[Double]]) => Unit) {
    val res = ins.toIterator.zip(xs.iterator).map {
      case (in, x) => {
        val modelBroadcast = in.context.broadcast(x)
        in.mapPartitions(rows => {
          Iterator(MatrixUtils.rowsToMatrix(rows) * modelBroadcast.value)
        })
      }
    }

    var prev: Option[RDD[DenseMatrix[Double]]] = None
    for (next <- res) {
      val sum = prev match {
        case Some(prevVal) => prevVal.zip(next).map(c => c._1 + c._2).cache()
        case None => next.cache()
      }

      evaluator.apply(sum.flatMap(MatrixUtils.matrixToRowArray))
      prev.map(_.unpersist())
      prev = Some(sum)
    }
    prev.map(_.unpersist())
  }
}

object BlockLinearMapper extends Serializable {

  def trainWithL2(
    trainingFeatures: Seq[RDD[DenseVector[Double]]],
    trainingLabels: RDD[DenseVector[Double]],
    lambda: Double,
    numIter: Int)
  : BlockLinearMapper = {
    // Find out numRows, numCols once
    val b = RowPartitionedMatrix.fromArray(trainingLabels.map(_.toArray)).cache()
    val numRows = Some(b.numRows())
    val numCols = Some(trainingFeatures.head.first().length.toLong)

    val A = trainingFeatures.map(rdd => {
      new RowPartitionedMatrix(rdd.mapPartitions { rows =>
        Iterator.single(MatrixUtils.rowsToMatrix(rows))
      }.map(RowPartition), numRows, numCols)
    })

    val bcd = new BlockCoordinateDescent()
    val models = bcd.solveLeastSquaresWithL2(A, b, Array(lambda), numIter, new NormalEquations()).transpose
    new BlockLinearMapper(models.head)
  }
}
