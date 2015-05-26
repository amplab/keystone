package nodes.learning

import breeze.linalg._
import edu.berkeley.cs.amplab.mlmatrix.{RowPartition, NormalEquations, BlockCoordinateDescent, RowPartitionedMatrix}
import nodes.stats.{StandardScalerModel, StandardScaler}
import org.apache.spark.rdd.RDD
import nodes.util.{VectorSplitter, Identity}
import pipelines.{Transformer, LabelEstimator}
import utils.{MatrixUtils, Stats}


/**
 * Transformer that applies a linear model to an input.
 * Different from [[LinearMapper]] in that the matrix representing the transformation
 * is split into a seq.
 * @param xs  The chunks of the matrix representing the linear model
 * @param blockSize blockSize to split data before applying transformations
 * @param bOpt optional intercept term to be added
 * @param featureScalersOpt optional seq of transformers to be applied before transformation
 */
class BlockLinearMapper(
    val xs: Seq[DenseMatrix[Double]],
    val blockSize: Int,
    val bOpt: Option[DenseVector[Double]] = None,
    val featureScalersOpt: Option[Seq[Transformer[DenseVector[Double], DenseVector[Double]]]] = None)
  extends Transformer[DenseVector[Double], DenseVector[Double]] {

  // Use identity nodes if we don't need to do scaling
  val featureScalers = featureScalersOpt.getOrElse(
    Seq.fill(xs.length)(new Identity[DenseVector[Double]]))
  val vectorSplitter = new VectorSplitter(blockSize)

  /**
   * Applies the linear model to feature vectors large enough to have been split into several RDDs.
   * @param in RDD of vectors to apply the model to
   * @return the output vectors
   */
  override def apply(in: RDD[DenseVector[Double]]): RDD[DenseVector[Double]] = {
    apply(vectorSplitter(in))
  }

  /**
   * Applies the linear model to feature vectors large enough to have been split into several RDDs.
   * @param ins RDD of vectors to apply the model to, split into same size as model blocks
   * @return the output vectors
   */
  def apply(in: Seq[RDD[DenseVector[Double]]]): RDD[DenseVector[Double]] = {
    val res = in.zip(xs.zip(featureScalers)).map {
      case (rdd, xScaler) => {
        val (x, scaler) = xScaler
        val modelBroadcast = rdd.context.broadcast(x)
        scaler(rdd).mapPartitions(rows => {
          if (!rows.isEmpty) {
            Iterator.single(MatrixUtils.rowsToMatrix(rows) * modelBroadcast.value)
          } else {
            Iterator.empty
          }
        })
      }
    }

    val matOut = res.reduceLeft((sum, next) => sum.zip(next).map(c => c._1 + c._2))

    // Add the intercept here
    val bBroadcast = matOut.context.broadcast(bOpt)
    val matOutWithIntercept = matOut.map { mat =>
      bOpt.map { b =>
        mat(*, ::) :+= b
        mat
      }.getOrElse(mat)
    }

    matOutWithIntercept.flatMap(MatrixUtils.matrixToRowArray)
  }

  override def apply(in: DenseVector[Double]): DenseVector[Double] = {
    val res = vectorSplitter.splitVector(in).zip(xs.zip(featureScalers)).map {
      case (in, xScaler) => {
        xScaler._1.t * xScaler._2(in)
      }
    }

    val out = res.reduceLeft((sum, next) => sum + next)
    bOpt.map { b =>
      out += b
      out
    }.getOrElse(out)
  }

  /**
   * Applies the linear model to feature vectors. After processing chunk i of every vector, applies
   * @param evaluator to the intermediate output vector.
   * @param in input RDD
   */
  def applyAndEvaluate(in: RDD[DenseVector[Double]], evaluator: (RDD[DenseVector[Double]]) => Unit) {
    applyAndEvaluate(vectorSplitter(in), evaluator)
  }

  /**
   * Applies the linear model to feature vectors. After processing chunk i of every vector, applies
   * @param evaluator to the intermediate output vector.
   * @param in sequence of input RDD chunks
   */
  def applyAndEvaluate(
      in: Seq[RDD[DenseVector[Double]]],
      evaluator: (RDD[DenseVector[Double]]) => Unit) {
    val res = in.zip(xs.zip(featureScalers)).map {
      case (rdd, xScaler) => {
        val modelBroadcast = rdd.context.broadcast(xScaler._1)
        xScaler._2(rdd).mapPartitions(rows => {
          val out = MatrixUtils.rowsToMatrix(rows) * modelBroadcast.value
          Iterator.single(out)
        })
      }
    }

    var prev: Option[RDD[DenseMatrix[Double]]] = None
    for (next <- res) {
      val sum = prev match {
        case Some(prevVal) => prevVal.zip(next).map(c => c._1 + c._2).cache()
        case None => next.cache()
      }

      // NOTE: We should only add the intercept once. So do it right before
      // we call the evaluator but don't cache this
      val sumAndIntercept = sum.map { mat =>
        bOpt.map { b =>
          mat(*, ::) :+= b
          mat
        }.getOrElse(mat)
      }
      evaluator.apply(sumAndIntercept.flatMap(MatrixUtils.matrixToRowArray))
      prev.map(_.unpersist())
      prev = Some(sum)
    }
    prev.map(_.unpersist())
  }
}

/**
 * Fits a least squares model using block coordinate descent with provided
 * training features and labels
 * @param blockSize size of block to use in the solver
 * @param numIter number of iterations of solver to run
 * @param lambda L2-regularization to use
 */
class BlockLeastSquaresEstimator(blockSize: Int, numIter: Int, lambda: Double = 0.0)
  extends LabelEstimator[DenseVector[Double], DenseVector[Double], DenseVector[Double]] {

  /**
   * Fit a model using blocks of features and labels provided.
   *
   * @param trainingFeatures feature blocks to use in RDDs.
   * @param trainingLabels RDD of labels to use.
   */
  def fit(
      trainingFeatures: Seq[RDD[DenseVector[Double]]],
      trainingLabels: RDD[DenseVector[Double]]): BlockLinearMapper = {
    val labelScaler = new StandardScaler(normalizeStdDev = false).fit(trainingLabels)
    // Find out numRows, numCols once
    val b = RowPartitionedMatrix.fromArray(
      labelScaler.apply(trainingLabels).map(_.toArray)).cache()
    val numRows = Some(b.numRows())
    val numCols = Some(blockSize.toLong)

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
    new BlockLinearMapper(models.head, blockSize, Some(labelScaler.mean), Some(featureScalers))
  }

  /**
   * Fit a model after splitting training data into appropriate blocks.
   *
   * @param trainingFeatures training data to use in one RDD.
   * @param trainingLabels labels for training data in a RDD.
   */
  override def fit(
      trainingFeatures: RDD[DenseVector[Double]],
      trainingLabels: RDD[DenseVector[Double]]): BlockLinearMapper = {
    fit(trainingFeatures, trainingLabels, None)
  }

  def fit(
      trainingFeatures: RDD[DenseVector[Double]],
      trainingLabels: RDD[DenseVector[Double]],
      numFeaturesOpt: Option[Int]): BlockLinearMapper = {
    val vectorSplitter = new VectorSplitter(blockSize, numFeaturesOpt)
    val featureBlocks = vectorSplitter.apply(trainingFeatures)
    fit(featureBlocks, trainingLabels)
  }
}
