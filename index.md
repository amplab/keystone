---
title: KeystoneML - Large scale end-to-end machine pipelines.
layout: default
---

# KeystoneML

KeystoneML is a [Scala](http://scala-lang.org/) software framework from the [UC Berkeley AMPLab](http://amplab.cs.berkeley.edu/) designed to simplify the construction of *large scale*, *end-to-end*, machine learning pipelines with [Apache Spark](http://spark.apache.org/).

KeystoneML makes constructing even complicated machine learning pipelines easy. Here's an example text categorization pipeline which creates NGram features and handles stemming and stopword removal.

{% highlight scala %}
//Todo : include sample text pipeline.
val pipeline = NGrams then Stuff
def fun(f: Int): Stuff = f.toStuff
{% endhighlight %}

Once a pipeline is specified, it must be `fit()` on training data.
{% highlight scala %}
//Show pipeline.fit
{% endhighlight %}

Once the pipeline has been fit on training data, you can apply it to test data and evaluate its effectiveness.
{% highlight scala %}
//Show categorize(rdd)
//And results of evaluator(rdd)
{% endhighlight %}

Or you can run it in another system on new samples of text - just like any other function.
{% highlight scala %}
//Show categorize("Some text")
{% endhighlight %}

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

## Building new pipelines

Now that you've seen an example pipeline, have a look at the [programming guide](programming_guide.html). 

After that, head over to the [API documentation](docs/).

## Contributing

KeystoneML is an Apache Licenesed open-source project and we welcome contributions.
Have a look at our [Github Issues page](http://github.com/amplab/keystone/issues) if you'd like to contribute, and feel free to fork the repo and submit a pull request!

