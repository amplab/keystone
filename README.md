# KeystoneML
The biggest, baddest pipelines around.


# Example pipeline

### Build the KeystoneML project

```
./sbt/sbt assembly
```

### Example: MNIST pipeline

```
# Get the data from S3
wget http://mnist-data.s3.amazonaws.com/train-mnist-dense-with-labels.data
wget http://mnist-data.s3.amazonaws.com/test-mnist-dense-with-labels.data

KEYSTONE_MEM=4g ./bin/run-pipeline.sh \
  pipelines.images.mnist.MnistRandomFFT \
  --trainLocation ./train-mnist-dense-with-labels.data \
  --testLocation ./test-mnist-dense-with-labels.data \
  --numFFTs 4 \
  --blockSize 2048
```

## Running with spark-submit

To run KeystoneML pipelines on large datasets you will need a [Spark](http://spark.apache.org) cluster. 
KeystoneML pipelines run on the cluster using
[spark-submit](http://spark.apache.org/docs/latest/submitting-applications.html).

You need to export `SPARK_HOME` to run KeystoneML using spark-submit. Having done
that you can similarly use run-pipeline.sh to launch your pipeline.

```
export SPARK_HOME=~/spark-1.3.1-bin-cdh4 # should match the version keystone is built with
KEYSTONE_MEM=4g ./bin/run-pipeline.sh \
  pipelines.images.mnist.MnistRandomFFT \
  --trainLocation ./train-mnist-dense-with-labels.data \
  --testLocation ./test-mnist-dense-with-labels.data \
  --numFFTs 4 \
  --blockSize 2048
```
