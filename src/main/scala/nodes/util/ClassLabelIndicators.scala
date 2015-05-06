package nodes.util

import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD
import pipelines._

case class ClassLabelIndicatorsFromIntLabels(numClasses: Int)
  extends Transformer[Int, DenseVector[Double]] {
  override def apply(in: RDD[Int]): RDD[DenseVector[Double]] = {
    in.map { label =>
      val indicatorVector = DenseVector.fill(numClasses, -1.0)
      indicatorVector(label) = 1.0
      indicatorVector
    }
  }
}

case class ClassLabelIndicatorsFromIntArrayLabels(numClasses: Int)
  extends Transformer[Array[Int], DenseVector[Double]] {
  override def apply(in: RDD[Array[Int]]): RDD[DenseVector[Double]] = {
    in.map { label =>
      val indicatorVector = DenseVector.fill(numClasses, -1.0)
      var i = 0
      while(i < label.length) {
        indicatorVector(i) = 1.0
        i+=1
      }
      indicatorVector
    }
  }
}