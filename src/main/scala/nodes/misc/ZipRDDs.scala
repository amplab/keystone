package nodes.misc

import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD
import pipelines.FunctionNode

/**
 * A node that zips DenseVectors in a Seq of RDDs, outputting a single RDD of the zipped DenseVectors
 */
object ZipRDDs extends FunctionNode[Seq[RDD[DenseVector[Double]]], RDD[DenseVector[Double]]] {
  def apply(in: Seq[RDD[DenseVector[Double]]]): RDD[DenseVector[Double]] = {
    in.reduceLeft((a,b) => a.zip(b).map(r => DenseVector.vertcat(r._1, r._2)))
  }
}
