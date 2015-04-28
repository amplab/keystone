package utils

import java.io.{FileWriter, File}
import java.util.{Random => JRandom}
import javax.imageio.ImageIO

import org.apache.commons.io.FileUtils
import org.apache.commons.math3.random.{RandomGenerator}

import pipelines._
import org.apache.spark.rdd.RDD
import breeze.linalg._
import breeze.numerics._
import breeze.stats._
import breeze.stats.distributions._

import scala.reflect.ClassTag

object Stats extends Serializable {
  /**
   * Margin to use for comparing numerical values.
   */
  var thresh = 1e-8

  /**
   * Compares two numbers for approximate equality. Modify margin by setting Stats.thresh.
   *
   * @param a A number.
   * @param b A second number.
   * @return True if the two numbers are within `thresh` of each other.
   */
  def aboutEq(a: Double, b: Double): Boolean = {
    math.abs(a-b) < thresh
  }

  /**
   * Compares two arrays for approximate equality. Modify margin by setting Stats.thresh.
   *
   * @param as A array of numbers.
   * @param bs A second array of numbers.
   * @return True if the two numbers are within `thresh` of each other.
   */
  def aboutEq(as: DenseVector[Double], bs: DenseVector[Double]): Boolean = {
    abs(as-bs).toArray.forall(_ < thresh)
  }

  /**
   * Compares two matrices for approximate equality.
   *
   * @param a A matrix.
   * @param b A second matrix.
   * @return True iff all corresponding elements of the two matrices are within `thresh` of each other.
   */
  def aboutEq(a: DenseMatrix[Double], b: DenseMatrix[Double]): Boolean = {
    require(a.rows == b.rows && a.cols == b.cols, "Matrices must be the same size.")

    abs(a-b).toArray.forall(_ < thresh)
  }
  /**
   *  Apply power normalization: z <- sign(z)|z|^{\rho}
   *  with \rho = \frac{1}{2}
   *  This a "signed square root"
   *  @param in  Input DenseVector[Double] RDD
   */
  def signedHellingerMapper(in: RDD[DenseVector[Double]]): RDD[DenseVector[Double]] = {
    in.map(x => {
      x.map(xi => math.signum(xi) * math.sqrt(math.abs(xi)))
    })
  }
  /**
    *  Generates a rows x cols Random matrix (dense) using Breeze's RandBasis (which takes in a prng)
    *  all values are uniformly distributed doubles between 0.0 and 1.0
    */
  def randMatrixUniform(rows: Int, cols: Int, r: RandBasis = Rand): DenseMatrix[Double] = {
    DenseMatrix.rand(rows, cols,r.uniform)
  }
  /**
    *  Generates a rows x cols Random matrix (dense) using Breeze's RandBasis (which takes in a prng)
    *  all values are Gaussian ("normally") distributed double value with mean 0.0 and standard deviation 1.0 
    */
  def randMatrixGaussian(rows: Int, cols: Int, r: RandBasis = Rand): DenseMatrix[Double] = {
    DenseMatrix.rand(rows, cols,r.gaussian)
  }
  
  /**
    *  Generates a rows x cols Random matrix (dense) using Breeze's RandBasis (which takes in a prng)
    *  all values are Cauchy distributed double value with location 0.0 and scale 1.0
    */
  def randMatrixCauchy(rows: Int, cols: Int, r: RandBasis = Rand): DenseMatrix[Double] = { 
    tan(math.Pi*(DenseMatrix.rand(rows, cols, r.uniform) - 0.5))
  }
}
