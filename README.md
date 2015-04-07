# sbt-spark-ec2
* A wrapper of [spark-ec2](http://spark.apache.org/docs/latest/ec2-scripts.html) that can be plugged into sbt and let sbt deploy spark cluster and run spark jobs easily on Amazon EC2.
* Currently supports Spark 1.3.0

## How to use this plugin
* Install [AWS CLI](http://aws.amazon.com/cli/): `pip install awscli` (require 1.6.2+)
* Set the environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` for AWS.
* In your sbt project, create `project/plugins.sbt`:

```
addSbtPlugin("net.pishen" % "sbt-spark-ec2" % "0.5.1")
```

* Create `spark-conf.json`:

```
{
  "cluster-name": "pishen-spark",
  "keypair": "pishen",
  "pem": "/home/pishen/.ssh/pishen.pem",
  "region": "us-west-2",
  "master-type": "m3.medium",
  "slave-type": "m3.medium",
  "num-of-slaves": 1,
  "main-class": "core.Main"
}
```

* Below is a more complex example of `spark-conf.json`:
```
{
  "cluster-name": "pishen-spark",
  "keypair": "pishen",
  "pem": "/home/pishen/.ssh/pishen.pem",
  "region": "us-west-2",
  "zone": "us-west-2b",
  "master-type": "m3.medium",
  "slave-type": "m3.medium",
  "num-of-slaves": 1,
  "main-class": "core.Main",
  "app-name": "my-spark-job",
  "spark-version": "1.3.0",
  "executor-memory": "1G",
  "vpc-id": "vpc-xxxxxxxx",
  "subnet-id": "subnet-xxxxxxxx",
  "use-private-ips": true
}
```

* Edit your job's algorithm.
* Launch Spark cluster by `sbt sparkLaunchCluster`, you can also execute `sbt` first and type `sparkLaunchCluster` (support TAB completion) in the sbt console.
* Once launched, submit your job by `sbt sparkSubmitJob <args>`

## All available commands
* `sbt sparkConf`
* `sbt sparkEc2Dir`
* `sbt sparkEc2Run`
* `sbt sparkLaunchCluster`
* `sbt sparkDestroyCluster`
* `sbt sparkStartCluster`
* `sbt sparkStopCluster`
* `sbt sparkShowMachines`
* `sbt sparkLoginMaster`
* `sbt sparkShowSpaceUsage` Show the i-node and disk usage of all the instances.
* `sbt sparkUploadJar`
* `sbt sparkSubmitJob <args>` Start the job remotely from local machine.
* `sbt sparkRemoveS3Dir <dir-name>` Remove the s3 directory with the `_$folder$` folder file. (ex. `sbt sparkRemoveS3Dir s3://bucket_name/middle_folder/target_folder`)
