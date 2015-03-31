package sbtspark

import sbt._
import sbt.Def.spaceDelimited
import Keys._
import sbtassembly.AssemblyKeys._
import scala.io.Source
import java.io.File
import java.nio.file._
import play.api.libs.json.Json
import play.api.libs.json.JsValue
import play.api.libs.json.JsObject

object SbtSparkPlugin extends AutoPlugin {

  object autoImport {
    lazy val sparkRunSparkEc2 = inputKey[Unit]("Run spark-ec2")

    lazy val sparkLaunchCluster = taskKey[Unit]("Launch new Spark cluster.")
    lazy val sparkDestroyCluster = taskKey[Unit]("Destroy existed Spark cluster.")
    lazy val sparkStartCluster = taskKey[Unit]("Start existed Spark cluster.")
    lazy val sparkStopCluster = taskKey[Unit]("Stop existed Spark cluster.")
    lazy val sparkShowMachines = taskKey[Unit]("Show the addresses of machines.")

    lazy val sparkLoginMaster = taskKey[Unit]("Login master with ssh.") //experimental
    lazy val sparkShowSpaceUsage = taskKey[Unit]("Show space usage for all the instances.")

    lazy val sparkSubmitJob = inputKey[Unit]("Upload and run the job directly.")

    lazy val sparkRemoveS3Dir = inputKey[Unit]("Remove the s3 directory include _$folder$ postfix file.")
  }
  import autoImport._
  override def trigger = allRequirements

  case class SparkConf(
    clusterName: String,
    keyPair: String,
    pemFile: String,
    region: String,
    zone: Option[String],
    masterType: String,
    slaveType: String,
    numOfSlave: Int,
    mainClass: String,
    sparkVersion: Option[String],
    driverMemory: Option[String],
    executorMemory: Option[String],
    vpcId: Option[String],
    subnetId: Option[String])

  def sparkConf(baseDir: File) = {
    val json = Json.parse(Source.fromFile(baseDir / "spark-conf.json").mkString)

    val pemFile = new File((json \ "pemFile").as[String])
    assert(pemFile.exists(), "I can't find your pem file at " + pemFile.getAbsolutePath)

    SparkConf(
      (json \ "clusterName").as[String],
      (json \ "keyPair").as[String],
      pemFile.getAbsolutePath,
      (json \ "region").as[String],
      (json \ "zone").asOpt[String],
      (json \ "masterType").as[String],
      (json \ "slaveType").as[String],
      (json \ "numOfSlave").as[Int],
      (json \ "mainClass").as[String],
      (json \ "sparkVersion").asOpt[String],
      (json \ "driverMemory").asOpt[String],
      (json \ "executorMemory").asOpt[String],
      (json \ "vpcId").asOpt[String],
      (json \ "subnetId").asOpt[String])
  }

  def runSparkEc2(args: Seq[String], targetDir: File) = {
    val ec2Dir = targetDir / "ec2"

    if (!ec2Dir.exists) {
      val gzFile = targetDir / "ec2.tar.gz"
      Files.copy(getClass.getResourceAsStream("/ec2.tar.gz"), gzFile.toPath, StandardCopyOption.REPLACE_EXISTING)
      Seq("tar", "-zxf", gzFile.getAbsolutePath, "--directory", gzFile.getParent).!
    }

    val cmd = Seq(ec2Dir.getAbsolutePath + "/spark-ec2") ++ args
    println(cmd.mkString(" "))
    val res = cmd.!<
    if (res != 0) sys.error("spark-ec2 return " + res) else res
  }

  def masterAddressOpt(clusterName: String) = getInstanceAddresses(clusterName + "-master").headOption

  def slaveAddresses(clusterName: String) = getInstanceAddresses(clusterName + "-slaves")

  def getInstanceAddresses(groupName: String) = {
    val jsonStr = Seq("aws", "ec2", "describe-instances").!!
    val json = Json.parse(jsonStr)
    (json \\ "Instances").flatMap(_.as[Seq[JsObject]])
      .filter(j => (j \ "SecurityGroups" \\ "GroupName").flatMap(_.asOpt[String]).contains(groupName))
      .flatMap(j => (j \ "PrivateIpAddress").asOpt[String])
  }

  override lazy val projectSettings = Seq(
    sparkRunSparkEc2 := {
      runSparkEc2(spaceDelimited().parsed, target.value)
    },
    sparkLaunchCluster := {
      val conf = sparkConf(baseDirectory.value)

      val base = Seq(
        "-k", conf.keyPair,
        "-i", conf.pemFile,
        "-r", conf.region,
        "-m", conf.masterType,
        "-t", conf.slaveType,
        "-s", conf.numOfSlave.toString(),
        "--copy-aws-credentials")

      val optional = Seq(
        conf.zone.map(z => Seq("-z", z)),
        conf.sparkVersion.map(v => Seq("-v", v)),
        conf.vpcId.map(id => Seq("--vpc-id=" + id)),
        conf.subnetId.map(id => Seq("--subnet-id=" + id))).flatten.flatten

      runSparkEc2(base ++ optional ++ Seq("launch", conf.clusterName), target.value)
    },
    sparkDestroyCluster := {
      val conf = sparkConf(baseDirectory.value)

      runSparkEc2(Seq(
        "-r", conf.region,
        "destroy", conf.clusterName), target.value)
    },
    sparkStartCluster := {
      val conf = sparkConf(baseDirectory.value)

      runSparkEc2(Seq(
        "-i", conf.pemFile,
        "-r", conf.region,
        "--copy-aws-credentials",
        "start", conf.clusterName), target.value)
    },
    sparkStopCluster := {
      val conf = sparkConf(baseDirectory.value)

      runSparkEc2(Seq(
        "-r", conf.region,
        "stop", conf.clusterName), target.value)
    },
    sparkShowMachines := {
      val conf = sparkConf(baseDirectory.value)

      masterAddressOpt(conf.clusterName) match {
        case None => println("\u001B[31mno master found.\u001B[0m")
        case Some(masterAddress) =>
          println("\u001B[36mmaster\u001B[0m " + masterAddress)
          println("command to login: ssh -i " + conf.pemFile + " root@" + masterAddress)
          println("web console: http://" + masterAddress + ":8080")
      }

      val slaveMessages = slaveAddresses(conf.clusterName).map("\u001B[32mslave\u001B[0m " + _)
      if (slaveMessages.isEmpty) {
        println("\u001B[31mno slave found.\u001B[0m")
      } else {
        println(slaveMessages.mkString("\n"))
      }
    },
    sparkLoginMaster := {
      val conf = sparkConf(baseDirectory.value)

      masterAddressOpt(conf.clusterName) match {
        case None =>
          println("\u001B[31mno master found.\u001B[0m")
        case Some(masterAddress) =>
          Seq("ssh", "-tt", "-i", conf.pemFile, s"root@$masterAddress").!<
      }
    },
    sparkShowSpaceUsage := {
      val conf = sparkConf(baseDirectory.value)

      def getSpaceUsage(address: String) = {
        def getDFResult(cmd: String) = {
          val res = Seq("ssh", "-i", conf.pemFile, s"root@$address", cmd).!!
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

      masterAddressOpt(conf.clusterName) match {
        case None =>
          println("\u001B[31mno master found.\u001B[0m")
        case Some(masterAddress) =>
          println("\u001B[36mmaster\u001B[0m " + masterAddress)
          println(getSpaceUsage(masterAddress))
      }

      println("Checking slaves...")

      val addresses = slaveAddresses(conf.clusterName)

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
    sparkSubmitJob := {
      println("\u001B[31mWARN: You're submitting job directly, please make sure you have a stable network connection.\u001B[0m")

      val conf = sparkConf(baseDirectory.value)

      masterAddressOpt(conf.clusterName) match {
        case None =>
          println("\u001B[31mno master found.\u001B[0m")
        case Some(address) =>
          val jar = assembly.value
          val uploadJarCmd = Seq("scp", "-i", conf.pemFile, jar.getAbsolutePath, s"root@$address:~/job.jar")
          println(uploadJarCmd.mkString(" "))
          uploadJarCmd.!

          val jobArgs = spaceDelimited().parsed

          val sparkSubmitCmdBase = Seq(
            "./spark/bin/spark-submit",
            "--class", conf.mainClass,
            "--master", s"spark://$address:7077")

          val sparkSubmitCmdOptional = Seq(
            conf.driverMemory.map(m => Seq("--driver-memory", m)),
            conf.executorMemory.map(m => Seq("--executor-memory", m))).flatten.flatten

          val sparkSubmitCmd = sparkSubmitCmdBase ++ sparkSubmitCmdOptional ++ Seq("job.jar") ++ jobArgs

          val sparkSubmitCmdStr = sparkSubmitCmd.mkString(" ")

          val triggerCmd = Seq("ssh", "-i", conf.pemFile, s"root@$address", sparkSubmitCmdStr)
          println(triggerCmd.mkString(" "))
          triggerCmd.!
      }
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