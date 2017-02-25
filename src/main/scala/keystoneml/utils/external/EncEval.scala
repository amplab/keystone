package keystoneml.utils.external

class EncEval extends Serializable {
  System.loadLibrary("ImageFeatures") // This will load libImageEncoders.{so,dylib} from the library path.

  /**
   * Compute a mixture of Gaussians given a set of sample points.
   * @param nGauss Number of Gaussians to estimate.
   * @param nDim Number of dimensions of each sample.
   * @param samples The samples (in sample-major order).
   * @return The Gaussians, their variances, and their weights in a single flat array. (Center-major order).
   */
  @native
  def computeGMM(nGauss: Int, nDim: Int, samples: Array[Float]): Array[Float]

  /**
   * Calculates Fisher Vectors for a set of descriptors given a GMM.
   *
   * @param means Means - flat array in center-major order.
   * @param dims Number of dimensions of each center.
   * @param numClusters Number of GMM cluster centers.
   * @param covariances The variances of the GMM centers in center-major order.
   * @param priors The weights of the GMM in center order.
   * @param dSiftDescriptors Bag of descriptors on which to compute the GMM.
   * @return The Fisher Vector for the input descriptors.
   */
  @native
  def calcAndGetFVs(means: Array[Float], dims: Int, numClusters: Int, covariances: Array[Float],
                    priors: Array[Float], dSiftDescriptors: Array[Float]) : Array[Float]
}