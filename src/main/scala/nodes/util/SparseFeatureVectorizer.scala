package nodes.util

import breeze.linalg.SparseVector
import pipelines.Transformer

/** A transformer which given a feature space, maps features of the form (feature id, value) into a sparse vector */
class SparseFeatureVectorizer[T](featureSpace: Map[T, Int]) extends Transformer[Seq[(T, Double)], SparseVector[Double]] {
  private def transformVector(in: Seq[(T, Double)], featureSpaceMap: Map[T, Int]): SparseVector[Double] = {
    val features = in.map(f => (featureSpaceMap.get(f._1), f._2))
        .filter(_._1.isDefined)
        .map(f => (f._1.get, f._2.toDouble))
    SparseVector(featureSpaceMap.size)(features:_*)
  }

  override def apply(in: Seq[(T, Double)]): SparseVector[Double] = {
    transformVector(in, featureSpace)
  }
}
