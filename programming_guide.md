---
title: KeystoneML Programming Guide
layout: default
---

# KeystoneML Programming Guide
This document covers key API concepts present in KeystoneML, and presents an overview of its components.

KeystoneML is a software framework designed to make building and deploying large scale machine learning pipelines easier.
To assist developers in this task we have created an API that simplifies common tasks and presents a unified interface
for all stages of the pipeline.

Additionally we've included a rich library of example pipelines and the operators (or *nodes*) that support them.

## Design Principles
KeystoneML is built on several design principles: supporting end-to-end workflows, type safety, horizontal scalability, and composibility.

By focusing on these principles, KeystoneML allows for the construction of complete, robust, large scale pipelines that are constructed from *reusable, understandable parts*.

We've done our best to adhere to these principles throughout the development of KeystoneML, and we hope that this translates to better applications that use it!

## Key API Concepts
At the center of KeystoneML are a handful of core API concepts that allow us to build complex machine learning pipelines out of simple parts: `pipelines`, `nodes`, `transformers`, and `estimators`.

### Pipelines
A `Pipeline` is a dataflow that takes some input data and maps it to some output data through a series of `nodes`. 
By design, these nodes can operate on one data item (for point lookup) or many data items: for batch model evaluation.

In a sense, a pipeline is just a function that is composed of simpler functions. Here's part of the `Pipeline` definition:

{% highlight scala %}
package workflow

trait Pipeline[A, B] {
  // ...
  def apply(in: A): B
  def apply(in: RDD[A]): RDD[B]
  // ...
}
{% endhighlight %}

From this we can see that a Pipeline has two type parameters: its input and output types.
We can also see that it has methods to operate on just a single input data item, or on a batch RDD of data items.

### Nodes
Nodes come in two flavors: `Transformers` and `Estimators`. 
`Transformers` are nodes which provide a unary function interface for both single items and `RDD` of the same type of item, while an `Estimator` produces a `Transformer` based on some training data.

#### Transformers
As already mentioned, a `Transformer` is the simplest type of node, and takes an input, and deterministically *transforms* it into an output. 
Here's an abridged definition of the `Transformer` class.

{% highlight scala %}
package workflow

abstract class Transformer[A, B : ClassTag] extends TransformerNode[B] with Pipeline[A, B] {
  def apply(in: A): B
  def apply(in: RDD[A]): RDD[B] = in.map(apply)
  //...
}
{% endhighlight %}

There are a few things going on in this class definition.
First: A Transformer has two type parameters: its input and output types.
Second, *every* Transformer extends TransformerNode, which is used internally by Keystone for Pipeline construction and execution. 
In turn TransformerNode extends Serializable, which means it can be written out and shipped over the network to run on any machine in a Spark cluster.
Third, it extends Pipeline because every Transformer can be treated as a full pipeline in it's own right.
Fourth, it is `abstract` because it has an `apply` method which needs to be filled out by the implementor.
Fifth, it provides a default implementation of `apply(in: RDD[A]): RDD[B]` which simply runs the single-item version on each item in an RDD.
Developers worried about performance of their transformers on bulk datasets are welcome to override this method, and we do so in KeystoneML with some frequency.

While transformers are unary functions, they themselves may be parameterized by more than just their input. 
To handle this case, transformers can take additional state as constructor parameters. Here's a simple transformer which will add a fixed vector from any vector it is fed as input. *(Note: we make use of [breeze](https://github.com/scalanlp/breeze) library for all local linear algebra operations.)*

{% highlight scala %}
import pipelines.Transformer
import breeze.linalg._

class Adder(vec: Vector[Double]) extends Transformer[Vector[Double], Vector[Double]] {
  def apply(in: Vector[Double]): Vector[Double] = in + vec
}
{% endhighlight %}

We can then create a new `Adder` and `apply` it to a `Vector` or `RDD[Vector]` just as you'd expect:

{% highlight scala %}
val vec = Vector(1.0,2.0,3.0)

val subber = new Adder(vec)

val res = subber(Vector(2.0,3.0,6.0)) //Returns Vector(3.0,5.0,9.0)
{% endhighlight %}

If you want to play around with defining new Transformers, you can do so at the scala console by typing `sbt/sbt console` in the KeystoneML project directory.

#### Estimators

`Estimators` are what puts the **ML** in KeystoneML.
An abridged `Estimator` interface looks like this:

{% highlight scala %}
package workflow

abstract class Estimator[A, B] extends EstimatorNode {
  protected def fit(data: RDD[A]): Transformer[A, B]
  // ...
}
{% endhighlight %}

That is `Estimator` takes in training data as an `RDD` to its `fit()` method, and outputs a Transformer. 
This may sound like abstract functional programming nonsense, but as we'll see this idea is pretty powerful. 

Let's consider a concrete example.
Suppose you have a big list of vectors and you want to subtract off the mean of each coordinate across all the vectors (and new ones that come from the same distribution).
You could create an `Estimator` to do this like so:

{% highlight scala %}
import pipelines.Estimator

object ScalerEstimator extends Estimator[Vector[Double], Vector[Double]] {
  def fit(data: RDD[Vector[Double]]): Adder = {
    val mean = data.reduce(_ + _)/data.count.toDouble    
    new Adder(-1.0 * mean)
  }
}
{% endhighlight %}

A couple things to notice about this example:

1. `fit` takes an RDD, and computes the mean of each coordinate using familiar Spark and breeze operations.
2. Adder satisfies the `Transformer[Vector[Double],Vector[Double]]` interface so we can return an adder from our `ScalerEstimator` estimator.
3. By multiplying the mean by `-1.0` we can reuse the `Adder` code we already wrote and it will work as expected.

Of course, KeystoneML already includes this functionality out of the box via the `StandardScaler` class, so you don't have to write it yourself!

In most cases, `Estimators` are things that estimate machine learning models - like a `LinearMapEstimator` which learns a standard linear model on training data.

### Chaining Nodes and Building Pipelines

Pipelines are created by chaining transformers and estimators with the `andThen` methods. Going back to a different part of the `Transformer` interface: 

{% highlight scala %}
package workflow

trait Pipeline[A, B] {
  //...
  final def andThen[C](next: Pipeline[B, C]): Pipeline[A, C] = //...
}
{% endhighlight %}

Ignoring the implementation, `andThen` allows you to take a pipeline and add another onto it, yielding a new `Pipeline[A,C]` which works by first applying the first pipeline (`A` => `B`) and then applying the `next` pipeline (`B` => `C`). 

This is where **type safety** comes in to ensure robustness. As your pipelines get more complicated, you may end up trying to chain together nodes that are incompatible, but the compiler won't let you. This is powerful, because it means that if your pipeline compiles, it is more likely to work when you go to run it at scale. Here's an example of a simple two stage pipeline that adds 4.0 to every coordinate of a 3-dimensional vector:

{% highlight scala %}
val pipeline = new Adder(Vector(1.0,2.0,3.0)) andThen new Adder(Vector(3.0,2.0,1.0))
{% endhighlight %}

Since sometimes transformers are just simple unary functions, you can also inline a Transformer definition. Here's a three-stage pipeline that adds 2.0 to each element of a vector, computes its sum, and then translates that to a string:
{% highlight scala %}
import breeze.linalg._

val pipeline = new Adder(Vector(2.0, 2.0, 2.0)) andThen Transformer(sum) andThen Transformer(_.toString)
{% endhighlight %}

You can *also* chain `Estimators` onto transformers via the `andThen (estimator, data)` or `andThen (labelEstimator, data, labels)` methods. The latter makes sense if you're training a supervised learning model which needs ground truth training labels.
Suppose you want to chain together a pipeline which takes a raw image, converts it to grayscale, and then fits a linear model on the pixel space, and returns the most likely class according to the linear model.

You can do this with some code that looks like the following:

{% highlight scala %}
val labels: RDD[Vector[Double]] = //...
val trainImages: RDD[Image] = //...

val pipe = GrayScaler andThen 
  ImageVectorizer andThen 
  (LinearMapEstimator(), trainImages, trainLabels) andThen 
  MaxClassifier
{% endhighlight %}

In this example `pipe` has a type Pipeline[Image, Int] and predicts the most likely class of an input image according to the model fit on the training data
While this pipeline won't give you a very high quality model (because pixels are bad predictors of an image class), it demonstrates the APIs.


## Included in KeystoneML
One of the main features of KeystoneML is the example pipelines and nodes it provides out of the box. These are designed to illustrate end-to-end real world pipelines in computer vision, speech recognition, and natural language processing.

### Pipelines
We've included several example pipelines:

* Vision - Pipelines on MNIST, CIFAR-10, VOC 2007, and ImageNet datasets are included which achieve near state-of-the-art performance for image classification.
* Speech - A [pipeline on featurized TIMIT data](http://www.ifp.illinois.edu/~huang146/papers/Kernel_DNN_ICASSP2014.pdf) using random features achieves state of the art performance for phoneme classification.
* NLP - A simple pipeline performing n-gram based document classification is included. We also include a [Stupid Backoff](http://www.aclweb.org/anthology/D07-1090.pdf) language model.

### Nodes
Example nodes fall in several categories:

* Vision - Feature Extractors and common image transformations for computer vision.
* NLP - Contains common text transformations such as tokenization, N-gram extraction, and more sophisticated transformation based on advanced language models such as Named Entity Recognition.
* Statistics - Contains common statistical transformations including scaling, sampling, counting, and more sophisticated transforms like the FFT.
* Learning - Contains libraries for machine learning, standard linear models with or without L2 regularization, naive Bayes, PCA, ZCA Whitening, and advanced block linear models that scale to massive feature spaces.
* Utilities - For caching, casting, and merging datasets, and supporting classification tasks.

For several nodes (particularly in images) we call into external libraries (both Java and C) that contain fast, high quality implementations of the nodes in question - pushing reuse across language boundaries.

Full documentation of the nodes is available in <a href="api/latest/">the scaladoc</a>.


### Data Loaders
A data loader is the entry point for your data into a batch training pipeline.
We've included several data loaders for datasets that correspond to the example pipelines we've included. 

Where possible, we redistribute the input data via Amazon S3. 
However, we lack data redistribution rights for some of the input datasets, so you'll need to secure access to these yourself.


### Evaluators
KeystoneML also provides several utilities for evaluating models once they've been trained. Computing metrics like precision, recall, and accuracy on a test set. 

Metrics are currently calculated for Binary Classification, Multiclass Classification, and Multilabel Classification, with more on the way.
