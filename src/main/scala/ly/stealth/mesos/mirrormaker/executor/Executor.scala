package ly.stealth.mesos.mirrormaker.executor

import java.io.{PrintWriter, StringWriter}
import ly.stealth.mesos.mirrormaker.Util.Str
import org.apache.log4j._
import org.apache.mesos.Protos._
import org.apache.mesos.{ExecutorDriver, MesosExecutorDriver}

object Executor extends org.apache.mesos.Executor {
  val logger: Logger = Logger.getLogger(this.getClass)

  def registered(driver: ExecutorDriver, executor: ExecutorInfo, framework: FrameworkInfo, slave: SlaveInfo): Unit = {
    logger.info("[registered] framework:" + Str.framework(framework) + " slave:" + Str.slave(slave))
  }

  def reregistered(driver: ExecutorDriver, slave: SlaveInfo): Unit = {
    logger.info("[reregistered] " + Str.slave(slave))
  }

  def disconnected(driver: ExecutorDriver): Unit = {
    logger.info("[disconnected]")
  }

  def launchTask(driver: ExecutorDriver, task: TaskInfo): Unit = {
    logger.info("[launchTask] " + Str.task(task))
    startServer(driver, task)
  }

  def startServer(driver: ExecutorDriver, task: TaskInfo): Unit = {
    val process = new MirrorMakerProcess(() => {
      val status = TaskStatus.newBuilder
        .setTaskId(task.getTaskId).setState(TaskState.TASK_RUNNING)
      driver.sendStatusUpdate(status.build)
    })

    new Thread("MirrorMakerServer") {
      override def run() {
        try {
          process.doWork(task.getData.toStringUtf8)
        } catch {
          case t: Throwable =>
            logger.warn("", t)
            t.printStackTrace()
            sendTaskFailed(driver, task, t)
        } finally {
          stopExecutor(driver)
        }
      }
    }.start()
  }

  def killTask(driver: ExecutorDriver, id: TaskID): Unit = {
    logger.info("[killTask] " + id.getValue)
    stopExecutor(driver, async = true)
  }

  def frameworkMessage(driver: ExecutorDriver, data: Array[Byte]): Unit = {
    logger.info("[frameworkMessage] " + new String(data))
    handleMessage(driver, new String(data))
  }

  def shutdown(driver: ExecutorDriver): Unit = {
    logger.info("[shutdown]")
    stopExecutor(driver)
  }

  def error(driver: ExecutorDriver, message: String): Unit = {
    logger.info("[error] " + message)
  }

  def main(args: Array[String]) {

    val driver = new MesosExecutorDriver(Executor)
    val status = if (driver.run eq Status.DRIVER_STOPPED) 0 else 1

    System.exit(status)
  }

  private def stopExecutor(driver: ExecutorDriver, async: Boolean = false): Unit = {
    def stop0(): Unit = {
      driver.stop()
    }

    if (async)
      new Thread("ExecutorStopper") {
        override def run(): Unit = stop0()
      }.start()
    else
      stop0()
  }

  private def handleMessage(driver: ExecutorDriver, message: String): Unit = {
    if (message == "stop") driver.stop()
  }

  private def sendTaskFailed(driver: ExecutorDriver, task: TaskInfo, t: Throwable) {
    val stackTrace = new StringWriter()
    t.printStackTrace(new PrintWriter(stackTrace, true))

    driver.sendStatusUpdate(TaskStatus.newBuilder
      .setTaskId(task.getTaskId).setState(TaskState.TASK_FAILED)
      .setMessage("" + stackTrace)
      .build
    )
  }
}