package ly.stealth.mesos.mirrormaker

import java.io.{File, PrintStream}

import com.twitter.util.Await
import ly.stealth.mesos.mirrormaker.Util.?
import scopt.Read

object Cli {
  val DefaultTaskCpus = 0.2
  val DefaultTaskMem = 256D
  val ApiEnvName = "MMM_API"

  private val out: PrintStream = System.out

  def main(args: Array[String]) {
    try {
      parser.parse(args, NoOpts) match {
        case Some(SchedulerOpts(cfgFile, consumerCfg, producerCfg)) =>
          handleScheduler(cfgFile, consumerCfg, producerCfg)

        case Some(StatusOpts(restApi)) =>
          handleStatus(restApi)

        case Some(AddOpts(restApi, numOfTasks, cpusOpt, memOpt)) =>
          handleAdd(restApi, numOfTasks, cpusOpt, memOpt)

        case Some(DeleteOpts(restApi, ids)) =>
          handleDelete(restApi, ids)

        case Some(NoOpts) =>
          printLine("Use one of the commands below.")
          parser.showUsage

        case None =>
          printLine("Failed to parse arguments.")
          parser.showUsage
      }
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        System.err.println("Error: " + e.getMessage)
        sys.exit(1)
    }
  }

  def handleScheduler(schedulerCfgFile: File, consumerCfgFile: File, producerCfgFile: File): Unit = {

    val schedulerConfig = Config(schedulerCfgFile)

    val consumerProps = Util.loadConfigFile(consumerCfgFile)
    val producerProps = Util.loadConfigFile(producerCfgFile)
    val scheduler = new Scheduler(schedulerConfig, consumerProps, producerProps)

    scheduler.start()
  }

  private def resolveRestApi(restApiOpt: Option[String]): String =
    Util.resolve(restApiOpt, ?(System.getenv(ApiEnvName)))
      .getOrElse(throw new CliError("Undefined API url. Either define cli option " +
      s"with --api or set an environment variable $ApiEnvName"))

  def handleStatus(restApiOpt: Option[String]) {
    val restApi = resolveRestApi(restApiOpt)
    printLine(s"Resolved REST API HTTP Server address to: $restApi")

    val restClient = new RestClient(restApi)

    val apiResponse = Await.result(restClient.status())

    apiResponse.value match {
      case Some(cluster) =>
        printCluster(cluster)
      case None =>
        printLine("Get status request failed due to internal sever error. Please retry.")
    }
  }

  private def printCluster(cluster: Cluster) {
    printLine("Cluster:")
    if (cluster.servers.isEmpty) printLine("No servers were added. Add servers with 'add' command.")
    else cluster.servers.foreach(s => printServer(s, 1))
  }

  private def printServer(server: Server, indent: Int = 0) {
    printLine("server:", indent)
    printLine(s"id: ${server.id}", indent + 1)
    printLine(s"state: ${server.state}", indent + 1)
    printTaskConfig(server.taskData, server.requestTemplate, indent + 1)
  }

  private def printTaskConfig(taskData: Option[TaskData], rt: RequestTemplate, indent: Int) {
    printLine("task configuration:", indent)
    val cpus = taskData.map(_.cpus).getOrElse(rt.cpus)
    val mem = taskData.map(_.mem).getOrElse(rt.mem)
    printLine(s"cpu: $cpus", indent + 1)
    printLine(s"mem: $mem", indent + 1)
  }


  def handleAdd(restApiOpt: Option[String], numOfTasks: Int, cpus: Option[Double], mem: Option[Double]) {
    val restApi = resolveRestApi(restApiOpt)
    printLine(s"Resolved REST API HTTP Server address to: $restApi")

    val restClient = new RestClient(restApi)

    val addServersRequest = AddServersRequest(numOfTasks, cpus.getOrElse(DefaultTaskCpus), mem.getOrElse(DefaultTaskMem))
    val apiResponse = Await.result(restClient.add(addServersRequest))

    if (apiResponse.success)
      printLine(s"Added $numOfTasks tasks successfully.")

    apiResponse.value match {
      case Some(cluster) =>
        printCluster(cluster)
      case None =>
        printLine("Add request failed due to internal sever error. Please retry.")
    }
  }

  def handleDelete(restApiOpt: Option[String], ids: Seq[Long]) {
    val restApi = resolveRestApi(restApiOpt)
    printLine(s"Resolved REST API HTTP Server address to: $restApi")

    val restClient = new RestClient(restApi)

    val deleteServersRequest = DeleteServersRequest(ids)
    val apiResponse = Await.result(restClient.delete(deleteServersRequest))

    if (apiResponse.success)
      printLine(s"Delete servers request (ids=${ids.mkString(",")}) was sent successfully.")

    apiResponse.value match {
      case Some(cluster) =>
        printCluster(cluster)
      case None =>
        printLine("Delete request failed due to internal sever error. Please retry.")
    }
  }

  private def printLine(s: AnyRef = "", indent: Int = 0) = out.println("  " * indent + s)

  def reads[A](f: String => A): Read[A] = new Read[A] {
    val arity = 1
    val reads = f
  }

  implicit val intOptRead: Read[Option[Int]] = reads { s => if (s == null) None else Some(s.toInt) }
  implicit val doubleOptRead: Read[Option[Double]] = reads { s => if (s == null) None else Some(s.toDouble) }
  implicit val stringOptRead: Read[Option[String]] = reads { s => Option(s) }

  sealed trait BaseOpts

  case class SchedulerOpts(configFile: File = null,
                           consumerConfigFile: File = null,
                           producerConfigFile: File = null) extends BaseOpts

  case class StatusOpts(restApi: Option[String] = None) extends BaseOpts

  case class AddOpts(restApi: Option[String] = None,
                     numOfTasks: Int = -1,
                     cpus: Option[Double] = None,
                     mem: Option[Double] = None) extends BaseOpts

  case class DeleteOpts(restApi: Option[String] = None,
                        ids: Seq[Long] = Seq.empty) extends BaseOpts

  // just to be used in parser.parse
  object NoOpts extends BaseOpts

  val parser = new scopt.OptionParser[BaseOpts]("mmm.sh") {

    override def showUsage {
      Cli.out.println(usage)
    }

    help("help").text("Prints this usage text.")

    cmd("scheduler").text("Starts the scheduler of the mirror-maker-mesos framework.").action { (_, c) =>
      SchedulerOpts()
    }.children(
        arg[File]("<config>").text("Path to scheduler configuration file.").action { (value, opts) =>
          opts.asInstanceOf[SchedulerOpts].copy(configFile = value)
        },

        opt[File]("consumer-config").abbr("c-c").required().text("Path to the Kafka consumer configuration file. Required.").action { (value, opts) =>
          opts.asInstanceOf[SchedulerOpts].copy(consumerConfigFile = value)
        },

        opt[File]("producer-config").abbr("p-c").required().text("Path to Kafka producer configuration file. Required.").action { (value, opts) =>
          opts.asInstanceOf[SchedulerOpts].copy(producerConfigFile = value)
        }
      )

    cmd("status").text("Print the cluster status").action { (_, c) =>
      StatusOpts()
    }.children(
        opt[Option[String]]("api").optional().text(s"REST API Http server address. E.g.: http://192.168.3.5:7000/api. Optional if $ApiEnvName is set.").action { (value, opts) =>
          opts.asInstanceOf[StatusOpts].copy(restApi = value)
        }
      )

    cmd("add").text("Add and start mirror maker instances").action { (_, c) =>
      AddOpts()
    }.children(
        arg[Int]("<num-of-tasks>").text("Number of tasks to be added").action { (value, opts) =>
          opts.asInstanceOf[AddOpts].copy(numOfTasks = value)
        }.validate(value => if (value > 0) success else failure("Value <num-of-tasks> must be greater than 0")),

        opt[Option[String]]("api").optional().text(s"REST API Http server address. E.g.: http://192.168.3.5:7000/api. Optional if $ApiEnvName is set.").action { (value, opts) =>
          opts.asInstanceOf[AddOpts].copy(restApi = value)
        },

        opt[Option[Double]]('c', "cpu").optional().text(s"Resources: 'cpu' to be allocated for the task. Default is '$DefaultTaskCpus'").action { (value, opts) =>
          opts.asInstanceOf[AddOpts].copy(cpus = value)
        },

        opt[Option[Double]]('m', "mem").optional().text(s"Resources: 'mem' to be allocated for the task. Default is '$DefaultTaskMem'").action { (value, opts) =>
          opts.asInstanceOf[AddOpts].copy(mem = value)
        }
      )

    cmd("delete").text("Stop and remove mirror maker instances by id").action { (_, c) =>
      DeleteOpts()
    }.children(
        arg[Long]("<task-id>").unbounded().text("Task id - can be checked from the 'status' command. Accepts multiple occurrences of this option.").action { (value, opts) =>
          opts.asInstanceOf[DeleteOpts].copy(ids = opts.asInstanceOf[DeleteOpts].ids :+ value)
        },

        opt[Option[String]]("api").optional().text(s"REST API Http server address. E.g.: http://192.168.3.5:7000/api. Optional if $ApiEnvName is set.").action { (value, opts) =>
          opts.asInstanceOf[DeleteOpts].copy(restApi = value)
        }
      )
  }

  case class CliError(message: String) extends RuntimeException(message)

}