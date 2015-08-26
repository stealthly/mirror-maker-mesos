package ly.stealth.mesos.mirrormaker

import org.apache.log4j.Logger
import org.apache.mesos.Protos.Offer
import scala.beans.BeanProperty
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object States {
  val Added = "Added"
  val Starting = "Starting"
  val Running = "Running"
  val Stopped = "Stopped"
  val Finished = "Finished"
}

case class TaskData(id: String, slaveId: String, executorId: String, attributes: Map[String, String],
                    cpus: Double, mem: Double)

case class Server(id: Long,
                  requestTemplate: RequestTemplate,
                  var state: String,
                  var taskData: Option[TaskData])

class Cluster {

  private val logger = Logger.getLogger(this.getClass)

  @BeanProperty val servers = ListBuffer.empty[Server]

  private def nextSeq: Long = servers.sortBy(_.id).lastOption.map(_.id + 1).getOrElse(1)

  def addRequest(requestTemplate: RequestTemplate): Unit = {
    val id = requestTemplate.id.getOrElse(nextSeq)
    if (servers.exists(_.id == id))
      throw new IllegalArgumentException(s"Server with id=$id already exists")

    servers += Server(id, requestTemplate, States.Added, None)
  }

  def applicableRequest(resourceOffer: ResourceOffer): Option[Server] = {
    servers.filter(s => s.state == States.Stopped || s.state == States.Added).find {
      server => resourceOffer.matches(server.requestTemplate)
    }
  }

  def getServerByTaskId(taskId: String): Option[Server] = {
    servers.find(
      server => server.taskData.exists(_.id == taskId)
    )
  }

  def getServer(id: Long): Option[Server] = {
    servers.find(
      server => server.id == id
    )
  }

  def serverStarting(server: Server, taskData: TaskData): Unit = {
    server.taskData = Some(taskData)
    changeServerStatus(server, States.Starting)
  }

  def deleteServer(server: Server): Unit = {
    val index = servers.indexWhere(_.id == server.id)
    if (index != -1) servers.remove(index)
  }


  /**
   * Added -> Running
   * Starting -> Running | Stopped
   * Stopped -> Starting
   * Running -> Stopped | Finished
   */
  def changeServerStatus(server: Server, newState: String): Unit = {
    val currentState = server.state
    logger.info(s"Changing server (id=${server.id}) state: $currentState -> $newState")

    def stateTransition(allowedTargetStates: Seq[String]): Unit = {
      require(allowedTargetStates.contains(newState), "Unexpected server (id=${server.id}) state transition $currentState -> $newState")
      server.state = newState
    }

    currentState match {
      case States.Added =>
        stateTransition(Seq(States.Starting))
      case States.Starting =>
        stateTransition(Seq(States.Running, States.Stopped))
      case States.Stopped =>
        stateTransition(Seq(States.Starting))
      case States.Running =>
        stateTransition(Seq(States.Stopped, States.Finished))
    }
  }

  def fullStatus =
    s"""
        |Cluster State:
        |-Added:    ${servers.filter(_.state == States.Added).mkString}
        |-Starting: ${servers.filter(_.state == States.Starting).mkString}
        |-Running:  ${servers.filter(_.state == States.Running).mkString}
        |-Stopped:  ${servers.filter(_.state == States.Stopped).mkString}
        |-Finished: ${servers.filter(_.state == States.Finished).mkString}
        |-Total:    ${servers.size}
    """.stripMargin

  def shortStatus = "Cluster State: Added(%d), Starting (%d), Running(%d), Stopped(%d), Finished(%d). Total(%d)"
    .format(servers.count(_.state == States.Added), servers.count(_.state == States.Starting),
      servers.count(_.state == States.Running), servers.count(_.state == States.Stopped),
      servers.count(_.state == States.Finished), servers.size)
}

object ResourceOffer {
  def fromMesosOffer(offer: Offer): ResourceOffer = {
    val offerResources = offer.getResourcesList.asScala.toList.map(res => res.getName -> res).toMap

    val cpus = offerResources.get("cpus").map(_.getScalar.getValue)
    val mem = offerResources.get("mem").map(_.getScalar.getValue)

    ResourceOffer(cpus, mem)
  }
}

case class ResourceOffer(cpus: Option[Double], mem: Option[Double]) {

  def matches(rt: RequestTemplate): Boolean =
    cpus.exists(_ > rt.cpus) && mem.exists(_ > rt.mem)

}

case class RequestTemplate(id: Option[Long],
                           cpus: Double, mem: Double)