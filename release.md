---
title: KeystoneML - Large scale end-to-end machine pipelines.
layout: default
---

# Release Notes

## Version 0.4 - 2017-03-02

We are pleased to announce the fourth major release of KeystoneML with version 0.4.0, available immediately on Maven Central. This release includes a number of important enhancehments, highlighted below. This version includes contributions from: Michael Franklin, Eric Jonas, Tomer Kaftan, Benjamin Recht, Rebecca Roelofs, Vaishaal Shankar, Evan Sparks, Stephen Tu, and Shivaram Venkataraman, consisting of 17 closed issues and pull requests. 

### Namespace Changes

Due to requests from several users, we have moved all KeystoneML namespaces to the top-level `keystoneml` namespace. For users, your imports will need to change from (e.g. `import pipelines._` to `import keystoneml.pipelines._`). 

### Spark 2 Support, Scala 2.11 and 2.10 compatibility.

KeystoneML now supports Spark 2.x only. The reasons for lack of backwards compatibility with the 1.x line of Spark is due to lack of backwards compatibility in SparkSQL and Breeze, which KeystoneML depends on. Users wishing to continue to use Spark 1.x are encouraged to remain on version 0.3. If a significant need for 1.x remains, we will consider cutting a separate branch, but maintenance of two such branches going forward will be difficult. Further, KeystoneML now has Scala 2.10 and 2.11 artifacts published on Maven Central.

### Optimizer Improvements

Tomer Kaftan led an overhaul of the optimizer internals. The biggest user-facing consequence of this change is that pipelines now in general return `PipelineDataset` objects, which are a lazy wrapper around RDDs, which can be accessed via the `PipelineDataset.get` method. This change is designed to give the optimizer more complete purview of the underlying application enabling better optimizations. These changes went through extensive design reviews and we are confident that they have made the system faster, more robust, and more maintainable.

### Kernel Methods

Several members of the KeystoneML team contributed to design and implementation of the Kernel Ridge Regression estimator. This is the first large scale kernel method available in the software. We have used it to solve problems where the kernel matrix is up to 2m x 2m. 

### Evaluator Interface

We have standardized the interface for Evaluators such that they all have predictable usage and types and work equally well with `RDD`s and `PipelineDataset`s.

## Version 0.3 - 2016-03-24

We are pleased to announce the third major release of KeystoneML with version 0.3.0, available immediately on Maven Central. This is the first release in 5 months, and carries with it a number of features, highlighted below. This release includes contributions from: Michael Franklin, Eric Jonas, Tomer Kaftan, Benjamin Recht, Vaishaal Shankar, Evan Sparks, Stephen Tu, and Shivaram Venkataraman, closing 28 issues, all tracked on Github. We expect to tighten the release cycle in future releases as the software becomes more mature.

### Optimizer Improvements
The largest set of improvements in KeystoneML v0.3.0 come in the optimizer, where we extend our use of a Catalyst-based optimizer to two major classes of optimizations: Node-level Optimization and Whole-pipeline Optimization.

### Node-level Optimization
Since pipelines are constructed using a high-level syntax, we can allow users to specify a *generic* node as part of their pipeline definition. For example, instead of specifying “BlockLeastSquaresEstimatorWithL2”, users can just specify “LeastSquaresEstimator” which tells the system that they don’t care how the model is trained, just that they want to fit a linear model to their data. The system will simulate the first several stages of the pipeline on a sample of data, and using a data sample, data statistics, and cluster properities, the “LeastSquaresEstimator” will attempt automatically select the fastest algorithm based on these properties.

More generally, if a user uses a node in their pipeline that extends the trait `Optimizable`, the optimizer will call into this node's `sample` method and the node will select the best implementation to use.

We’ve also included a node-level optimizer for PCA and GMM in addition to the one for the Least Squares Solver.

### Whole-Pipeline Optimization
Another feature we’ve introduced in v0.3.0 is Whole-Pipeline optimization. This is an optimization step that occurs after node-level optimization, and is concerned with executing the pipeline given the pipleine DAG in as efficient a way as possible. In its current iteration, the whole-pipeline optimizer inspects the pipeline DAG and automatically decides where to cache intermediate output using a greedy algorithm. It exploits knowledge of the DAG structure and uses samples of the data as it goes through the pipeline to project memory utilization and runtime when the pipeline runs on the complete workload. We’ve validated this algorithm on a number of sample pipelines, but for now it is an optional feature while we collect information about its effectiveness on more workloads.

### New Solvers
A number of new solvers for large linear systems are introduced in this release. In particular, we now have support for multiclass least-squares classifiers solved with L-BFGS, and an optimized implementation of the block-coordinate descent solver when number of classes is small (the existing implementation was ideal in the case when #classes was in the hundreds or thousands). Other included solvers include a batch gradient solver, local least squares solver, sparse batch gradient, sparse L-BFGS, and a fast regularized dual solver which works well in the case that n << d. 

The least-squares optimizer discussed above will dynamically call out to the full suite of these solvers in a future release.

### New Operators
We’ve included a number of new Operators in this release. These include a Scala-native GMM implementation which is faster than the existing C-version for small numbers of Gaussians, a node for Image Cropping, the hashing trick for text featurization, summary information for binary classifiers, and a variety of implementations of PCA.

In particular, we’ve included both local and distributed PCA implementations, as well as a distributed approximate PCA algorithm which can be significantly faster than the exact algorithm when the number of principal components requested is small relative to the number of features in the matrix.

### Performance Improvements
We’ve improved performance of a number of commonly used nodes like the Convolver and Pooler by aligning memory-access with physical layout, leading to a 5x improvement in node-level performance in some cases.
 
### Bug Fixes and Extra Testing
Based on user feedback, we’ve been able to catch and fix a number of bugs in the software. Significant fixes were made to handle edge cases with empty partitions or irregularly sized blocks in the block solver and the vector splitter. The interfaces between Daisy and SIFT were unified so that each is a drop-in replacement for the other. A workaround for stack issues in Multi-BLAS in OpenBLAS was included in the run scripts. 

Unit tests have been expanded to catch edge cases that bit users and additional input validation was added to some operators.

Finally, we’ve constructed a suite of integration tests which we will run periodically on a live Spark cluster to monitor for performance regressions. These tests have been run on the AMPLab local cluster and we’ve verified that this release of KeystoneML is the fastest one yet!

### KeystoneML v0.4 and Beyond
We are working on several new features in KeystoneML v0.4 including hyperparameter tuning support, advanced application profiling and job placement algorithms, and expanded large-scale linear algebra support. We’re also working on including a Kernel Ridge Regression Solver based on [this paper](http://arxiv.org/abs/1602.05310), which should come sooner the v0.4. Stay tuned to Github for updates.

*Please keep the bug reports and pull requests coming!*



## Version 0.2 - 2015-09-18

* Some pipeline APIs have been updated and moved to a `workflow` module. In general, pipeline construction is more consistent and uses `andThen` regardless of what you are chaining to a pipeline.
* `Pipeline.gather` operator added to simplify combining multiple pipeline branches.
* Examples updated to reflect these API changes.
* Bug fixes in example pipelines.
* A fix was added to make weighted least squares solver work on problems with one block.
* Fixes to build process.
* Amazon Reviews example classification pipeline added.
* Contributors include: Tomer Kaftan, Evan Sparks, and Shivaram Venkataraman.
