package nodes.learning

/**
 * A trait that represents a known system performance cost model for a solver.
 */
trait CostModel {
  def cost(
    n: Long,
    d: Int,
    k: Int,
    sparsity: Double,
    numMachines: Int,
    cpuWeight: Double,
    memWeight: Double,
    networkWeight: Double)
  : Double
}