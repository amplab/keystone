package nodes.learning

import breeze.linalg._
import edu.berkeley.cs.amplab.mlmatrix.{RowPartition, NormalEquations, BlockCoordinateDescent, RowPartitionedMatrix}
import nodes.stats.{StandardScalerModel, StandardScaler}
import org.apache.spark.rdd.RDD

import nodes.util.Identity
import pipelines.Transformer
import utils.{MatrixUtils, Stats}


/**
 * Transformer that applies a linear model to an input.
 * Different from [[LinearMapper]] in that the matrix representing the transformation
 * is split into a seq, and the vectors being transformed are likewise expected to have
 * been split into a Seq, matching the split of the transformation matrix.
 * @param xs  The chunks of the matrix representing the linear model
 */
class BlockLinearMapper(
    val xs: Seq[DenseMatrix[Double]],
    val bOpt: Option[DenseVector[Double]] = None,
    val featureScalersOpt: Option[Seq[StandardScalerModel]] = None)
  extends Transformer[Seq[DenseVector[Double]], DenseVector[Double]] {

  // Use identity nodes if we don't need to do scaling
  val featureScalers = featureScalersOpt.getOrElse(
    Seq.fill(xs.length)(new Identity[DenseVector[Double]]))

  /**
   * Applies the linear model to feature vectors large enough to have been split into several RDDs.
   * @param ins  RDDs where the RDD at index i is expected to contain chunk i of all input vectors.
   *             Must all be zippable.
   * @return the output vectors
   */
  def apply(ins: TraversableOnce[RDD[DenseVector[Double]]]): RDD[DenseVector[Double]] = {
    val res = ins.toIterator.zip(xs.iterator.zip(featureScalers.iterator)).map {
      case (in, xScaler) => {
        val x = xScaler._1
        val scaler = xScaler._2
        val modelBroadcast = in.context.broadcast(x)
        val bBroadcast = in.context.broadcast(bOpt)
        scaler.apply(in).mapPartitions(rows => {
          if (!rows.isEmpty) {
            val out = MatrixUtils.rowsToMatrix(rows) * modelBroadcast.value
            Iterator.single(
              bBroadcast.value.map { b =>
                out(*, ::) :+= b
                out
              }.getOrElse(out)
            )
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
    val res = ins.zip(xs.zip(featureScalers)).map {
      case (in, xScaler) => {
        val out = xScaler._1.t * xScaler._2.apply(in)
        bOpt.map { b =>
          out :+= b
          out
        }.getOrElse(out)
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
    val res = ins.toIterator.zip(xs.iterator.zip(featureScalers.iterator)).map {
      case (in, xScaler) => {
        val x = xScaler._1
        val scaler = xScaler._2
        val modelBroadcast = in.context.broadcast(x)
        val bBroadcast = in.context.broadcast(bOpt)
        scaler.apply(in).mapPartitions(rows => {
          val out = MatrixUtils.rowsToMatrix(rows) * modelBroadcast.value
          Iterator.single(
            bBroadcast.value.map { b =>
              out(*, ::) :+= b
              out
            }.getOrElse(out)
          )
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

    val labelScaler = new StandardScaler(normalizeStdDev = false).fit(trainingLabels)

    // Find out numRows, numCols once
    val b = RowPartitionedMatrix.fromArray(
      labelScaler.apply(trainingLabels).map(_.toArray)).cache()
    val numRows = Some(b.numRows())
    val numCols = Some(trainingFeatures.head.first().length.toLong)

    // NOTE: This will cause trainingFeatures to be evaluated twice
    // which might not be optimal if its not cached ?
    val featureScalers = trainingFeatures.map { rdd =>
      new StandardScaler(normalizeStdDev = false).fit(rdd)
    }

    val A = trainingFeatures.zip(featureScalers).map { case (rdd, scaler) =>
      new RowPartitionedMatrix(scaler.apply(rdd).mapPartitions { rows =>
        Iterator.single(MatrixUtils.rowsToMatrix(rows))
      }.map(RowPartition), numRows, numCols)
    }

    val bcd = new BlockCoordinateDescent()
    val models = bcd.solveLeastSquaresWithL2(
      A, b, Array(lambda), numIter, new NormalEquations()).transpose
    new BlockLinearMapper(models.head, Some(labelScaler.mean), Some(featureScalers))
  }
}
