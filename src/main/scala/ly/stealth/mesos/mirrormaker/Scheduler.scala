package ly.stealth.mesos.mirrormaker

import java.io.File
import java.util
import java.util.{Properties, UUID}

import com.google.protobuf.ByteString
import Util.Str
import org.apache.log4j.Logger
import org.apache.mesos.Protos._
import org.apache.mesos.{MesosSchedulerDriver, SchedulerDriver}

import scala.collection.JavaConverters._

class Scheduler(val config: Config,
                val consumerProps: Properties,
                val producerProps: Properties) extends org.apache.mesos.Scheduler {

  private val logger = Logger.getLogger(this.getClass)

  private var driver: SchedulerDriver = null

  var cluster = new Cluster

  var maxTasks = 3

  def start() {
    logger.info(s"Starting ${getClass.getSimpleName}")

    val frameworkBuilder = FrameworkInfo.newBuilder()
    frameworkBuilder.setUser("")
    frameworkBuilder.setName(config.FrameworkName)
    frameworkBuilder.setFailoverTimeout(30 * 24 * 3600)
    frameworkBuilder.setCheckpoint(true)

    val driver = new MesosSchedulerDriver(this, frameworkBuilder.build, config.MesosMaster)

    val httpServer = new HttpServer(this)
    httpServer.start()

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        if (driver != null) {
          logger.info("Stopping driver")
          driver.stop()
        }
        if (httpServer != null) {
          logger.info("Stopping http server")
          httpServer.stop()
        }
      }
    })

    val status = if (driver.run eq Status.DRIVER_STOPPED) 0 else 1
    System.exit(status)
  }

  override def registered(driver: SchedulerDriver, id: FrameworkID, master: MasterInfo) {
    logger.info("[registered] framework:" + Str.id(id.getValue) + " master:" + Str.master(master))

    this.driver = driver
  }

  override def offerRescinded(driver: SchedulerDriver, id: OfferID) {
    logger.info("[offerRescinded] " + Str.id(id.getValue))
  }

  override def disconnected(driver: SchedulerDriver) {
    logger.info("[disconnected]")

    this.driver = null
  }

  override def reregistered(driver: SchedulerDriver, master: MasterInfo) {
    logger.info("[reregistered] master:" + Str.master(master))

    this.driver = driver
  }

  override def slaveLost(driver: SchedulerDriver, id: SlaveID) {
    logger.info("[slaveLost] " + Str.id(id.getValue))
  }

  override def error(driver: SchedulerDriver, message: String) {
    logger.info("[error] " + message)
  }

  override def statusUpdate(driver: SchedulerDriver, status: TaskStatus) {
    logger.info("[statusUpdate] " + Str.taskStatus(status))

    onServerStatus(driver, status)
  }

  override def frameworkMessage(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, data: Array[Byte]) {
    logger.info("[frameworkMessage] executor:" + Str.id(executorId.getValue) + " slave:" + Str.id(slaveId.getValue) + " data: " + new String(data))
  }

  override def resourceOffers(driver: SchedulerDriver, offers: util.List[Offer]): Unit = {
    logger.debug("[resourceOffers]\n" + Str.offers(offers.asScala))

    onResourceOffers(offers.asScala.toList)
  }

  private def onServerStatus(driver: SchedulerDriver, status: TaskStatus) {
    logger.debug(cluster.shortStatus)
    val server = cluster.getServerByTaskId(status.getTaskId.getValue)

    status.getState match {
      case TaskState.TASK_RUNNING =>
        onServerStarted(server, driver, status)
      case TaskState.TASK_LOST | TaskState.TASK_FAILED | TaskState.TASK_ERROR =>
        onServerFailed(server, status)
      case TaskState.TASK_FINISHED =>
        onServerFinished(server, status)
      case TaskState.TASK_KILLED =>
        logger.info(s"Task ${status.getTaskId.getValue} has been killed")
      case _ => logger.warn("Got unexpected task state: " + status.getState)
    }
  }

  private def onServerStarted(serverOpt: Option[Server], driver: SchedulerDriver, status: TaskStatus) {
    serverOpt match {
      case Some(server) if server.state != States.Running =>
        cluster.changeServerStatus(server, States.Running)
      case Some(server) => //status update for a running server
      case None =>
        logger.info(s"Got ${status.getState} for unknown/stopped server, killing task ${status.getTaskId}")
        driver.killTask(status.getTaskId)
    }
  }

  private def onServerFailed(serverOpt: Option[Server], status: TaskStatus) {
    serverOpt match {
      case Some(server) if server.state != States.Stopped =>
        cluster.changeServerStatus(server, States.Stopped)
      case Some(server) => // status update for a stopped server
      case None => logger.info(s"Got ${status.getState} for unknown/stopped server with task ${status.getTaskId}")
    }
  }

  private def onServerFinished(serverOpt: Option[Server], status: TaskStatus) {
    serverOpt match {
      case Some(server) if server.state != States.Finished =>
        cluster.changeServerStatus(server, States.Finished)
      case Some(server) => // status update for a finished server
      case None =>
        logger.info(s"Got ${status.getState} for unknown/stopped server with task ${status.getTaskId}")
    }
  }

  private def onResourceOffers(offers: List[Offer]) {
    logger.debug(cluster.shortStatus)

    var changed = false

    offers.foreach { offer =>
      val resourceOffer = ResourceOffer.fromMesosOffer(offer)

      cluster.applicableRequest(resourceOffer) match {
        case Some(server) =>
          logger.info(s"Accepting offer ${offer.getId.getValue} to serve ${server.requestTemplate}")
          launchTask(offer, server)
          changed = true
        case None =>
          logger.debug(s"Declining offer ${offer.getId.getValue}")
          driver.declineOffer(offer.getId)
      }
    }

    if (changed) logger.info(cluster.fullStatus)
    else logger.debug(cluster.shortStatus)
  }

  private def launchTask(offer: Offer, server: Server) {
    val mesosTask = buildTask(offer, server)

    driver.launchTasks(util.Arrays.asList(offer.getId), util.Arrays.asList(mesosTask))
    val taskData = TaskData(mesosTask.getTaskId.getValue, mesosTask.getSlaveId.getValue, mesosTask.getExecutor.getExecutorId.getValue, Map.empty,
      server.requestTemplate.cpus, server.requestTemplate.mem)

    cluster.serverStarting(server, taskData)
  }

  implicit class RichTaskInfoBuilder(taskBuilder: TaskInfo.Builder) {
    val CpusResource = "cpus"
    val MemResource = "mem"

    private def withScalarResource(name: String, value: Double): TaskInfo.Builder =
      taskBuilder.addResources(Resource.newBuilder.setName(name).setType(Value.Type.SCALAR).setScalar(Value.Scalar.newBuilder.setValue(value)))

    def withCpus(cpus: Double): TaskInfo.Builder = withScalarResource(CpusResource, cpus)

    def withMem(mem: Double): TaskInfo.Builder = withScalarResource(MemResource, mem)
  }


  private def buildTask(offer: Offer, server: Server): TaskInfo = {
    def taskData: ByteString = {
      val data = Map(
        "offset.commit.interval.ms" -> config.OffsetCommitIntervalMs,
        "consumerProps" -> Util.formatMap(consumerProps.asScala),
        "producerProps" -> Util.formatMap(producerProps.asScala),
        "topics" -> Util.formatList(config.Topics.map(x => (x, ""))))

      ByteString.copyFromUtf8(Util.formatMap(data))
    }

    val taskBuilder: TaskInfo.Builder = TaskInfo.newBuilder
      .setName("mmm-task")
      .setTaskId(TaskID.newBuilder.setValue(UUID.randomUUID().toString).build)
      .setData(taskData)
      .setSlaveId(offer.getSlaveId)
      .setExecutor(newExecutor(server.id))

    taskBuilder
      .withCpus(server.requestTemplate.cpus)
      .withMem(server.requestTemplate.mem)
      .build
  }

  private def newExecutor(serverId: Long): ExecutorInfo = {
    val cmd = "java -Dlog4j.configuration=mmm-log4j.properties -cp mmm-0.1-SNAPSHOT.jar ly.stealth.mesos.mirrormaker.executor.Executor"

    val commandBuilder = CommandInfo.newBuilder
    commandBuilder
      .addUris(CommandInfo.URI.newBuilder().setValue(new File("dist/mmm-0.1-SNAPSHOT.jar").getAbsolutePath))
      .addUris(CommandInfo.URI.newBuilder().setValue(new File("dist/mmm-log4j.properties").getAbsolutePath))
      .setValue(cmd)

    ExecutorInfo.newBuilder()
      .setName(s"mmm-$serverId")
      .setExecutorId(ExecutorID.newBuilder.setValue(serverId.toString))
      .setCommand(commandBuilder)
      .build()
  }


  override def executorLost(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, status: Int) {
    logger.info("[executorLost] executor:" + Str.id(executorId.getValue) + " slave:" + Str.id(slaveId.getValue) + " status:" + status)
  }

  def onAddServer(request: AddServersRequest): Unit = {
    logger.info(s"Handling $request")
    request.asRequestTemplates.foreach(cluster.addRequest)
  }

  def onDeleteServer(request: DeleteServersRequest): Unit = {
    logger.info(s"Handling $request")
    request.ids.foreach {
      id =>
        val server = cluster.getServer(id)
        val taskIdOpt = server.flatMap(_.taskData.map(_.id))
        taskIdOpt.foreach(taskId => driver.killTask(TaskID.newBuilder().setValue(taskId).build()))
        server.foreach(cluster.deleteServer)
    }
  }
}