---
title: KeystoneML - Large scale end-to-end machine pipelines.
layout: default
---

# KeystoneML
[![Build Status](https://amplab.cs.berkeley.edu/jenkins/job/KeystoneML/badge/icon)](https://amplab.cs.berkeley.edu/jenkins/job/KeystoneML/)

KeystoneML is a software framework, written in [Scala](http://scala-lang.org/), from the [UC Berkeley AMPLab](http://amplab.cs.berkeley.edu/) designed to simplify the construction of *large scale*, *end-to-end*, machine learning pipelines with [Apache Spark](http://spark.apache.org/).

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
{% endhighlight %}

The result of this code is the following:
{% highlight bash %}
a    b    c    d    e    f    g    h    i    j    k    l    m    n    o    p    q    r    s    t    <-- Classified As             
--   --   --   --   --   --   --   --   --   --   --   --   --   --   --   --   --   --   --   --   --                            
276  6    24   8    34   4    2    0    0    4    9    2    5    6    2    0    0    0    5    2    a = comp.graphics             
44   158  109  16   23   3    1    6    0    5    3    3    8    3    7    1    0    0    1    3    b = comp.os.ms-windows.misc   
16   5    304  18   6    6    0    0    0    1    25   0    3    6    1    0    0    0    1    0    c = comp.sys.ibm.pc.hardware  
7    1    35   303  3    5    2    4    0    0    10   1    5    7    1    1    0    0    0    0    d = comp.sys.mac.hardware     
38   5    16   2    320  0    1    1    0    2    1    3    4    1    0    0    0    0    0    1    e = comp.windows.x            
2    0    6    4    0    358  4    2    0    0    9    1    2    6    1    1    0    0    0    0    f = rec.autos                 
1    1    3    3    1    18   357  1    0    1    3    0    0    5    4    0    0    0    0    0    g = rec.motorcycles           
7    0    1    2    0    4    1    350  19   0    3    0    0    3    2    1    0    0    2    2    h = rec.sport.baseball        
1    1    1    0    1    1    1    9    379  0    1    1    0    0    0    0    0    0    0    3    i = rec.sport.hockey          
11   1    3    7    1    1    0    1    0    354  7    3    0    1    0    4    0    0    2    0    j = sci.crypt                 
21   1    34   17   4    10   3    0    1    9    281  6    2    3    0    0    0    0    0    1    k = sci.electronics           
13   0    5    9    3    14   4    0    2    1    15   296  4    7    5    1    3    1    5    8    l = sci.med                   
10   0    2    0    1    3    0    0    0    0    5    5    352  2    7    3    0    0    1    3    m = sci.space                 
5    2    16   4    0    5    1    0    1    0    9    2    2    343  0    0    0    0    0    0    n = misc.forsale              
2    0    0    2    0    3    1    0    1    4    1    0    9    0    194  84   1    2    1    5    o = talk.politics.misc        
0    0    1    0    0    2    3    0    0    5    1    2    2    2    4    337  0    4    0    1    p = talk.politics.guns        
1    0    0    1    1    0    3    1    1    2    1    0    1    1    12   2    333  0    7    9    q = talk.politics.mideast     
3    0    0    0    1    3    0    1    0    0    1    5    4    2    7    11   1    127  45   40   r = talk.religion.misc        
2    0    1    0    0    1    1    0    1    2    1    7    5    1    0    1    2    12   263  19   s = alt.atheism               
5    0    3    0    3    0    0    0    1    0    1    2    2    1    2    0    1    4    4    369  t = soc.religion.christian    
--   --   --   --   --   --   --   --   --   --   --   --   --   --   --   --   --   --   --   --   --                            
Avg Accuracy:	0.980
Macro Precision:0.816
Macro Recall:	0.797
Macro F1:	0.797
Total Accuracy:	0.804
Micro Precision:0.804
Micro Recall:	0.804
Micro F1:	0.804
     

{% endhighlight %}

This relatively simple pipeline predicts the right document category over 80% of the time on the test set.

Of course, you can the pipeline in another system on new samples of text - just like any other function.
{% highlight scala %}
println(newsgroupsData.classes(predictor("The Philadelphia Phillies win the World Series!")))

//Prints out:
//rec.sport.baseball
{% endhighlight %}

KeystoneML works with much more than just text. Have a look at our [examples](examples.html) to see pipelines in the domains of computer vision and speech.

KeystoneML is alpha software, in its very first public release (v0.1).
The project is still very young, but we feel that it has grown to the point where it is viable for general use.

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
$ make
{% endhighlight %}

This will automatically resolve dependencies and package a jar file in `target/keystone/scala-2.10/keystone-assembly-0.1.jar`.

You can then run example pipelines with the included `bin/run-pipeline.sh` script, or pass as an argument to `spark-submit`.


## Running an Example

Once you've built KeystoneML, you can run many of the example pipelines locally.
However, to run the larger examples, you'll want access to a Spark cluster.

Here's an example of running a handwriting recognition pipeline on the popular MNIST dataset. 
You should be able to run this on a single machine in under a minute.

{% highlight bash %}
# Get the data from S3
wget http://mnist-data.s3.amazonaws.com/train-mnist-dense-with-labels.data
wget http://mnist-data.s3.amazonaws.com/test-mnist-dense-with-labels.data

KEYSTONE_MEM=4g ./bin/run-pipeline.sh \
  pipelines.images.mnist.MnistRandomFFT \
  --trainLocation ./train-mnist-dense-with-labels.data \
  --testLocation ./test-mnist-dense-with-labels.data \
  --numFFTs 4 \
  --blockSize 2048
{% endhighlight %}

To run on a cluster, we recommend using the `spark-ec2` to launch a cluster and provision with correct versions of [BLAS](http://www.netlib.org/blas/) and native C libraries used by KeystoneML.

We've provided some scripts to set up a well-configured cluster automatically in `bin/pipelines-ec2.sh`. You can read more about using them [here](running_pipelines.html).

## Building New Pipelines

Now that you've seen an example pipeline, have a look at the [programming guide](programming_guide.html). 

After that, head over to the [API documentation](api/).

## Contributing

KeystoneML is an Apache Licensed open-source project and we welcome contributions.
Have a look at our [Github Issues page](http://github.com/amplab/keystone/issues) if you'd like to contribute, and feel free to fork the repo and submit a pull request!

