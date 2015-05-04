# sbt-spark-ec2
* A wrapper of [spark-ec2](http://spark.apache.org/docs/latest/ec2-scripts.html) that can be plugged into sbt and let sbt deploy spark cluster and run spark jobs easily on Amazon EC2.
* Currently supports to Spark 1.3.1

## How to use this plugin
* Set the environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` for AWS.
* In your sbt project, create `project/plugins.sbt`:

```
addSbtPlugin("net.pishen" % "sbt-spark-ec2" % "0.7.1")
```

* Create `sbt-spark-ec2.conf`:

```
cluster-name = "pishen-spark"

keypair = "pishen"
pem = "/home/pishen/.ssh/pishen.pem"

region = "us-west-2"
# zone is optional
zone = "us-west-2a"

master-type = "m3.medium"
slave-type = "m3.medium"
num-of-slaves = 1

main-class = "mypackage.Main"

# optional configurations
app-name = "my-spark-job"
spark-version = "1.3.1"
driver-memory = "1G"
executor-memory = "1G"
vpc-id = "vpc-xxxxxxxx"
subnet-id = "subnet-xxxxxxxx"
use-private-ips = true
# delete the security groups when destroying cluster (optional)
delete-groups = true
```
* Create `build.sbt` (Here we give a simple example):
```
lazy val root = (project in file(".")).settings(
    name := "my-project-name",
    version := "0.1",
    scalaVersion := "2.10.5",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "1.3.1" % "provided"
    )
  )
```
* Write your job's algorithm in `src/main/scala/mypackage/Main.scala`:
```scala
package mypackage

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Main {
  def main(args: Array[String]) {
    //setup spark
    val sc = new SparkContext(new SparkConf())
    //your algorithm 
    sc.textFile("s3n://my-bucket/input.gz").collect().foreach(println)
  }
}
```
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
