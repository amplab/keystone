package workflow.graph

/**
 * The default Pipeline optimizer used when executing pipelines.
 */
object DefaultOptimizer extends Optimizer {
  protected val batches: Seq[Batch] =
    Batch("Load Saved State", Once, ExtractSaveablePrefixes, SavedStateLoadRule, DanglingNodeRemovalRule) ::
    Batch("Common Sub-expression Elimination", FixedPoint(Int.MaxValue), EquivalentNodeMergeRule) ::
      Nil
}
