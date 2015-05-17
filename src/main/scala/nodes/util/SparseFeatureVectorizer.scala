package nodes.util

import breeze.linalg.SparseVector
import pipelines.Transformer

/** A transformer which given a feature space, maps features of the form (feature id, value) into a sparse vector */
class SparseFeatureVectorizer(featureSpace: Map[Any, Int]) extends Transformer[Seq[(Any, Double)], SparseVector[Double]] {
  private def transformVector(in: Seq[(Any, Double)], featureSpaceMap: Map[Any, Int]): SparseVector[Double] = {
    val features = in.map(f => (featureSpaceMap.get(f._1), f._2))
        .filter(_._1.isDefined)
        .map(f => (f._1.get, f._2.toDouble))
    SparseVector(featureSpaceMap.size)(features:_*)
  }

  override def apply(in: Seq[(Any, Double)]): SparseVector[Double] = {
    transformVector(in, featureSpace)
  }
}
