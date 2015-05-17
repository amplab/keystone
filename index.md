---
title: KeystoneML - Large scale end-to-end machine pipelines.
layout: default
---

# KeystoneML
[![Build Status](https://amplab.cs.berkeley.edu/jenkins/job/KeystoneML/badge/icon)](https://amplab.cs.berkeley.edu/jenkins/job/KeystoneML/)

KeystoneML is a [Scala](http://scala-lang.org/) software framework from the [UC Berkeley AMPLab](http://amplab.cs.berkeley.edu/) designed to simplify the construction of *large scale*, *end-to-end*, machine learning pipelines with [Apache Spark](http://spark.apache.org/).

KeystoneML makes constructing even complicated machine learning pipelines easy. Here's an example text categorization pipeline which creates bigram features and creates a Naive Bayes model based on the 100,000 most common features.

{% highlight scala %}
val newsgroupsData = NewsGroupsDataLoader(sc, trainingDir, testingDir)

val predictor = Trim.then(LowerCase())
  .then(Tokenizer())
  .then(new NGramsFeaturizer(1 to 2)).to[Seq[Any]]
  .then(TermFrequency(x => 1))
  .thenEstimator(CommonSparseFeatures(100000))
  .fit(newsgroupsData.train.data).to[Vector[Double]]
  .thenLabelEstimator(NaiveBayesEstimator(numClasses))
  .fit(newsgroupsData.train.data, newsgroupsData.train.labels)
  .then(MaxClassifier)
{% endhighlight %}

Parallelization of the pipeline fitting process is handled automatically and pipeline nodes are designed to scale horizontally.

Once the pipeline has been fit on training data, you can apply it to test data and evaluate its effectiveness.
{% highlight scala %}
val testLabels = newsgroupsData.test.labels
val testResults = predictor(newsgroupsData.test.data)
val eval = MulticlassClassifierEvaluator(testResults, testLabels, numClasses)

println(eval.summary(newsgroupsData.classes))
//Prints out:
//Micro Precision: 0.8037705788635157
//Micro Recall: 0.8037705788635157
//Micro F1: 0.8037705788635157
{% endhighlight %}

This relatively simple pipeline predicts the right document category over 80% of the time on the test set.

Of course, you can the pipeline in another system on new samples of text - just like any other function.
{% highlight scala %}
println(newsgroupsData.classes(predictor("The Philadelphia Phillies win the World Series!")))

//Prints out:
//rec.sport.baseball
{% endhighlight %}

KeystoneML works with much more than just text. Have a look at our [examples](examples.html) to see pipelines in the domains of computer vision and speech.

## Downloading

KeystoneML is available on [GitHub](http://github.com/amplab/keystone/). 

{% highlight bash %}
$ git clone https://github.com/amplab/keystone.git
{% endhighlight %}


## Building

Once downloaded, you can build KeystoneML with the following commands:
{% highlight bash %}
$ cd keystone
$ sbt/sbt assembly
{% endhighlight %}

This will automatically resolve dependencies and package a jar file in `target/keystone/scala-2.10/keystone-assembly-0.1.jar`.

## Running Examples

Once you've built KeystoneML, you can run many of the example pipelines locally.
However, to run the larger examples, you'll want access to a Spark cluster.

{% highlight bash %}
$ #TODO example of running keystone.
{% endhighlight %}

## Building New Pipelines

Now that you've seen an example pipeline, have a look at the [programming guide](programming_guide.html). 

After that, head over to the [API documentation](docs/).

## Contributing

KeystoneML is an Apache Licenesed open-source project and we welcome contributions.
Have a look at our [Github Issues page](http://github.com/amplab/keystone/issues) if you'd like to contribute, and feel free to fork the repo and submit a pull request!

