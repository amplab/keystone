---
title: Running KeystoneML on EC2
layout: default
---

# Running KeystoneML on EC2

To run KeystoneML on EC2 you can use the
[spark-ec2](http://spark.apache.org/docs/latest/ec2-scripts.html) scripts.

## Getting spark-ec2

As the KeystoneML scripts require a recent version of spark-ec2 (Spark 1.4.0 or later), it is
recommended that you clone the Spark source code master branch for this. You can do this with
{% highlight bash %}
git clone https://github.com/apache/spark.git
{% endhighlight %}

## Launching a Cluster

You can now use the `bin/keystone-ec2.sh` to launch a cluster with KeystoneML pre-installed.
To do that you can run a command which looks like 

{% highlight bash %}
SPARK_EC2_DIR=<path_to_your_spark>/ec2 ./bin/keystone-ec2.sh \
  -s 4 \
  -t r3.4xlarge \
  -i <key-file> \
  -k <key-name> \
  launch keystone-test-cluster
{% endhighlight %}

The above command launches 4 slaves and 1 master machine of type r3.4xlarge.
Note that you can pass in any spark-ec2 options (like spot-prices etc.) to this script.

## Running KeystoneML on the cluster

Once the cluster launch finishes you can login to the master node and the KeystoneML
repository should be present in `/root/keystone`.
