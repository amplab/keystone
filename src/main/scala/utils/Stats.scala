package utils

import java.io.{FileWriter, File}
import java.util.{Random => JRandom}
import javax.imageio.ImageIO

import org.apache.commons.io.FileUtils
import pipelines._
import org.apache.spark.rdd.RDD
import breeze.linalg._
import breeze.numerics._
import breeze.stats._
import breeze.stats.distributions._

import scala.reflect.ClassTag

object Stats {
  val cauchy = new CauchyDistribution(0,1)
  val uniform = new Uniform(0,1)
  val gaussian = new Gaussian(0,1)
  /**
    *  Generates a rows x cols Random matrix (dense) using java's pseudo rng
    *  all values are uniformly distributed doubles between 0.0 and 1.0
    */
  def randMatrixUniform(rows: Int, cols: Int, r: Uniform = uniform): DenseMatrix[Double] = {
    DenseMatrix.rand(rows, cols,r)
  }
  /**
    *  Generates a rows x cols Random matrix (dense) using java's pseudo rng
    *  all values are Gaussian ("normally") distributed double value with mean 0.0 and standard deviation 1.0 
    */
  def randMatrixGaussian(rows: Int, cols: Int, r: Gaussian = gaussian): DenseMatrix[Double] = {
    DenseMatrix.rand(rows, cols,r)
  }
  
  /**
    *  Generates a rows x cols Random matrix (dense) using java's pseudo rng
    *  all values are Cauchy distributed double value with location 0.0 and scale 1.0
    */
  def randMatrixCauchy(rows: Int, cols: Int, r: CauchyDistribution = cauchy): DenseMatrix[Double] = { 
    DenseMatrix.rand(rows, cols,r)
  }
}
