/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sbtsparkec2

import sbt._
import sbt.Def.spaceDelimited
import Keys._
import sbtassembly.AssemblyKeys._
import scala.io.Source
import java.io.File
import java.nio.file._
import play.api.libs.json._

object SbtSparkPlugin extends AutoPlugin {

  object autoImport {
    lazy val sparkConf = settingKey[SparkConf]("Parse the configuration.")
    lazy val sparkEc2Dir = settingKey[File]("ec2 directory")
    lazy val sparkEc2Run = inputKey[Unit]("Run spark-ec2")

    lazy val sparkLaunchCluster = taskKey[Unit]("Launch new Spark cluster.")
    lazy val sparkDestroyCluster = taskKey[Unit]("Destroy existed Spark cluster.")
    lazy val sparkStartCluster = taskKey[Unit]("Start existed Spark cluster.")
    lazy val sparkStopCluster = taskKey[Unit]("Stop existed Spark cluster.")
    lazy val sparkShowMachines = taskKey[Unit]("Show the addresses of machines.")

    lazy val sparkLoginMaster = taskKey[Unit]("Login master with ssh.")
    lazy val sparkShowSpaceUsage = taskKey[Unit]("Show space usage for all the instances.")

    lazy val sparkUploadJar = taskKey[Unit]("Upload job jar to master.")
    lazy val sparkSubmitJob = inputKey[Unit]("Upload and run the job directly.")

    lazy val sparkRemoveS3Dir = inputKey[Unit]("Remove the s3 directory include _$folder$ postfix file.")
  }
  import autoImport._
  override def trigger = allRequirements

  case class SparkConf(
    clusterName: String,
    keypair: String,
    pem: String,
    region: String,
    zone: Option[String],
    masterType: String,
    slaveType: String,
    numOfSlaves: Int,
    mainClass: String,
    appName: Option[String],
    sparkVersion: Option[String],
    driverMemory: Option[String],
    executorMemory: Option[String],
    vpcId: Option[String],
    subnetId: Option[String],
    usePrivateIpsRaw: Option[Boolean]) {
    val usePrivateIps = if (usePrivateIpsRaw.getOrElse(false)) Some("--private-ips") else None
  }

  def runSparkEc2(args: Seq[String], targetDir: File) = {
    val ec2Dir = targetDir / "ec2"

    val cmd = Seq(ec2Dir.getAbsolutePath + "/spark-ec2") ++ args
    println(cmd.mkString(" "))
    val res = cmd.!<
    if (res != 0) sys.error("spark-ec2 return " + res) else res
  }

  def masterAddressOpt(conf: SparkConf) = getInstanceAddresses("-master", conf).headOption

  def slaveAddresses(conf: SparkConf) = getInstanceAddresses("-slaves", conf)

  def getInstanceAddresses(clusterNamePostfix: String, conf: SparkConf) = {
    val jsonStr = Seq("aws", "ec2", "describe-instances").!!
    val json = Json.parse(jsonStr)
    (json \\ "Instances").flatMap(_.as[Seq[JsObject]])
      .filter {
        instance =>
          (instance \ "State" \ "Name").as[String] == "running" &&
            (instance \ "SecurityGroups" \\ "GroupName").flatMap(_.asOpt[String]).contains(conf.clusterName + clusterNamePostfix)
      }
      .flatMap {
        instance =>
          if (conf.usePrivateIpsRaw.getOrElse(false)) {
            (instance \ "PrivateIpAddress").asOpt[String]
          } else {
            (instance \ "PublicIpAddress").asOpt[String]
          }
      }
  }

  override lazy val projectSettings = Seq(
    sparkConf := {
      val json = Json.parse(Source.fromFile(baseDirectory.value / "spark-conf.json").mkString)

      val pemFile = new File((json \ "pem").as[String])
      assert(pemFile.exists(), "I can't find your pem file at " + pemFile.getAbsolutePath)

      SparkConf(
        (json \ "cluster-name").as[String],
        (json \ "keypair").as[String],
        pemFile.getAbsolutePath,
        (json \ "region").as[String],
        (json \ "zone").asOpt[String],
        (json \ "master-type").as[String],
        (json \ "slave-type").as[String],
        (json \ "num-of-slaves").as[Int],
        (json \ "main-class").as[String],
        (json \ "app-name").asOpt[String],
        (json \ "spark-version").asOpt[String],
        (json \ "driver-memory").asOpt[String],
        (json \ "executor-memory").asOpt[String],
        (json \ "vpc-id").asOpt[String],
        (json \ "subnet-id").asOpt[String],
        (json \ "use-private-ips").asOpt[Boolean])
    },
    sparkEc2Dir := {
      IO.createDirectory(target.value)
      val ec2Dir = target.value / "ec2"
      IO.delete(ec2Dir)
      val gzFile = target.value / "ec2.tar.gz"
      Files.copy(getClass.getResourceAsStream("/ec2.tar.gz"), gzFile.toPath, StandardCopyOption.REPLACE_EXISTING)
      Seq("tar", "-zxf", gzFile.getAbsolutePath, "--directory", gzFile.getParent).!
      ec2Dir
    },
    sparkEc2Run := {
      runSparkEc2(spaceDelimited().parsed, target.value)
    },
    sparkLaunchCluster := {
      val conf = sparkConf.value

      val base = Seq(
        "-k", conf.keypair,
        "-i", conf.pem,
        "-r", conf.region,
        "-m", conf.masterType,
        "-t", conf.slaveType,
        "-s", conf.numOfSlaves.toString(),
        "--copy-aws-credentials")

      val optional = Seq(
        conf.zone.map(z => Seq("-z", z)),
        conf.sparkVersion.map(v => Seq("-v", v)),
        conf.vpcId.map(id => Seq("--vpc-id=" + id)),
        conf.subnetId.map(id => Seq("--subnet-id=" + id))).flatten.flatten

      runSparkEc2(base ++ optional ++ conf.usePrivateIps ++ Seq("launch", conf.clusterName), target.value)
    },
    sparkDestroyCluster := {
      val conf = sparkConf.value

      runSparkEc2(Seq(
        "-r", conf.region,
        "destroy", conf.clusterName) ++ conf.usePrivateIps, target.value)
    },
    sparkStartCluster := {
      val conf = sparkConf.value

      runSparkEc2(Seq(
        "-i", conf.pem,
        "-r", conf.region,
        "--copy-aws-credentials",
        "start", conf.clusterName) ++ conf.usePrivateIps, target.value)
    },
    sparkStopCluster := {
      val conf = sparkConf.value

      runSparkEc2(Seq(
        "-r", conf.region,
        "stop", conf.clusterName) ++ conf.usePrivateIps, target.value)
    },
    sparkShowMachines := {
      val conf = sparkConf.value

      masterAddressOpt(conf) match {
        case None => println("\u001B[31mno master found.\u001B[0m")
        case Some(masterAddress) =>
          println("\u001B[36mmaster\u001B[0m " + masterAddress)
          println("command to login: ssh -i " + conf.pem + " root@" + masterAddress)
          println("web console: http://" + masterAddress + ":8080")
      }

      val slaveMessages = slaveAddresses(conf).map("\u001B[32mslave\u001B[0m " + _)
      if (slaveMessages.isEmpty) {
        println("\u001B[31mno slave found.\u001B[0m")
      } else {
        println(slaveMessages.mkString("\n"))
      }
    },
    sparkLoginMaster := {
      val conf = sparkConf.value

      masterAddressOpt(conf) match {
        case None =>
          println("\u001B[31mno master found.\u001B[0m")
        case Some(masterAddress) =>
          Seq("ssh", "-tt", "-o", "StrictHostKeyChecking=no", "-i", conf.pem, s"root@$masterAddress").!<
      }
    },
    sparkShowSpaceUsage := {
      val conf = sparkConf.value

      def getSpaceUsage(address: String) = {
        def getDFResult(cmd: String) = {
          val res = Seq("ssh", "-o", "StrictHostKeyChecking=no", "-i", conf.pem, s"root@$address", cmd).!!
          res.split("\n").tail.map(_.split("\\s+").takeRight(2)).map(arr => arr(1) -> arr(0))
        }
        val usageStr = (getDFResult("df -i") zip getDFResult("df")).map {
          case ((m1, inodeUse), (m2, spaceUse)) =>
            def colorIfFull(str: String) = if (str.init.toInt >= 90) "\u001B[31m" + str + "\u001B[0m" else str
            Seq(colorIfFull(inodeUse), colorIfFull(spaceUse), m1).mkString("\t")
        }.mkString("\n")
        "i-node\tspace\tmount\n" + usageStr
      }

      println("Checking master...")

      masterAddressOpt(conf) match {
        case None =>
          println("\u001B[31mno master found.\u001B[0m")
        case Some(masterAddress) =>
          println("\u001B[36mmaster\u001B[0m " + masterAddress)
          println(getSpaceUsage(masterAddress))
      }

      println("Checking slaves...")

      val addresses = slaveAddresses(conf)

      if (addresses.isEmpty) {
        println("\u001B[31mno slave found.\u001B[0m")
      } else {
        addresses.foreach {
          address =>
            println("\u001B[32mslave\u001B[0m " + address)
            println(getSpaceUsage(address))
        }
      }
    },
    sparkUploadJar := {
      val conf = sparkConf.value

      masterAddressOpt(conf) match {
        case None =>
          sys.error("\u001B[31mno master found.\u001B[0m")
        case Some(address) =>
          val jar = assembly.value
          val uploadJarCmd = Seq("rsync", "--progress", "-ve", "ssh -o StrictHostKeyChecking=no -i " + conf.pem, jar.getAbsolutePath, s"root@$address:~/job.jar")
          println(uploadJarCmd.mkString(" "))
          uploadJarCmd.!
          println(s"Jar uploaded, you can login master (with sparkLoginMaster) and submit the job by yourself.")
      }
    },
    sparkSubmitJob := {
      println("\u001B[31mWARN: You're submitting job directly, please make sure you have a stable network connection.\u001B[0m")

      sparkUploadJar.value

      val conf = sparkConf.value

      val address = masterAddressOpt(conf).get

      val jobArgs = spaceDelimited().parsed

      val sparkSubmitCmdBase = Seq(
        "./spark/bin/spark-submit",
        "--class", conf.mainClass,
        "--master", s"spark://$address:7077")

      val sparkSubmitCmdOptional = Seq(
        conf.appName.map(n => Seq("--name", n)),
        conf.driverMemory.map(m => Seq("--driver-memory", m)),
        conf.executorMemory.map(m => Seq("--executor-memory", m))).flatten.flatten

      val sparkSubmitCmd = sparkSubmitCmdBase ++ sparkSubmitCmdOptional ++ Seq("job.jar") ++ jobArgs

      val sparkSubmitCmdStr = sparkSubmitCmd.mkString(" ")

      val triggerCmd = Seq("ssh", "-i", conf.pem, s"root@$address", sparkSubmitCmdStr)
      println(triggerCmd.mkString(" "))
      triggerCmd.!
    },
    sparkRemoveS3Dir := {
      val args = spaceDelimited().parsed
      require(args.length == 1, "Please give the directory name.")
      val dir = args.head
      val dirWithoutEndSlash = if (dir.endsWith("/")) dir.init else dir
      Seq("aws", "s3", "rm", dirWithoutEndSlash, "--recursive").!
      Seq("aws", "s3", "rm", dirWithoutEndSlash + "_$folder$").!
    })
}