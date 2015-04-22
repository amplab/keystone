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

object Stats {
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
