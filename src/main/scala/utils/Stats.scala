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

import scala.reflect.ClassTag

object Stats {
  /**
    *  Generates a rows x cols Random matrix (dense) using java's pseudo rng
    *  all values are uniformly distributed doubles between 0.0 and 1.0
    */
  def randMatrixUniform(rows: Int, cols: Int, r: JRandom = new java.util.Random()): DenseMatrix[DataType] = {
    val m = new DenseMatrix[DataType](rows, cols)

    for (i <- 0 to  (rows * cols) - 1) {
      m.data(i) = r.nextDouble()
    }
    m  
  }

  /**
    *  Generates a rows x cols Random matrix (dense) using java's pseudo rng
    *  all values are Gaussian ("normally") distributed double value with mean 0.0 and standard deviation 1.0 
    */
  def randMatrixGaussian(rows: Int, cols: Int, r: JRandom = new java.util.Random() ): DenseMatrix[DataType] = {
    val m = new DenseMatrix[DataType](rows, cols)
    for (i <- 0 to (rows * cols) - 1) {
      m.data(i) = r.nextGaussian()
    }
    m
  }
  
  /**
    *  Generates a rows x cols Random matrix (dense) using java's pseudo rng
    *  all values are Cauchy distributed double value with location 0.0 and scale 1.0
    */
  def randMatrixCauchy(rows: Int, cols: Int, r: JRandom = new java.util.Random()): DenseMatrix[DataType] = {
    val m = new DenseMatrix[DataType](rows, cols)
    for (i <- 0 to (rows * cols) - 1) {
      // Get a random number betwee -Pi/2 to Pi/2
      val angle = (r.nextDouble() - 1.0) * math.Pi / 2
      m.data(i) = math.tan(angle)
    }
    m
  }
}
