package nodes.images

import breeze.linalg.DenseVector

//Todo: We will need a concrete implementation of this sooner or later.
abstract class ZCAWhitener {
  val m: DenseVector[Double]
}