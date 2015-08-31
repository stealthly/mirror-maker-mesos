package ly.stealth.mesos.mirrormaker

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.twitter.util.{Try, Await, Future}
import io.finch._
import io.finch.request._
import io.finch.request.items.{BodyItem, ParamItem}
import io.finch.response._
import io.finch.route._
import com.twitter.finagle.{ListeningServer, SimpleFilter, Service, Httpx}
import com.twitter.finagle.httpx.{Request, Response}
import io.finch.jackson._
import org.apache.log4j.Logger

object ApiResponse {
  def apply(message: String): ApiResponse = ApiResponse(success = false, message, None)
}

case class ApiResponse(success: Boolean, message: String, value: Option[Cluster])

case class AddServersRequest(amount: Int,
                             cpus: Double, mem: Double) {

  def asRequestTemplates: Seq[RequestTemplate] =
    List.fill(amount)(
        RequestTemplate(None, cpus, mem)
    )

}

case class DeleteServersRequest(
                                 @JsonDeserialize(contentAs = classOf[java.lang.Long])
                                 ids: Seq[Long]
                                 )

trait ErrorHandling {

  implicit val objectMapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def errorHandler: PartialFunction[Throwable, Response] = {
    case RouteNotFound(route) => NotFound(
      ApiResponse(s"error -> route_not_found , route -> $route")
    )
    case NotPresent(ParamItem(p)) => BadRequest(
      ApiResponse(s"error -> param_not_present , param -> $p")
    )
    case NotPresent(BodyItem) => BadRequest(
      ApiResponse("error -> body_not_present")
    )
    case NotParsed(ParamItem(p), _, _) => BadRequest(
      ApiResponse(s"error -> param_not_parsed , param -> $p")
    )
    case NotParsed(BodyItem, _, _) => BadRequest(
      ApiResponse("error -> body_not_parsed")
    )
    case NotValid(ParamItem(p), rule) => BadRequest(
      ApiResponse(s"error -> param_not_valid , param -> $p, rule -> $rule")
    )
    case error: Throwable => NotFound(
      ApiResponse(s"error -> ${error.getMessage}")
    )
  }

  def handleExceptions: SimpleFilter[Request, Response] = new SimpleFilter[Request, Response] {
    def apply(req: Request, service: Service[Request, Response]): Future[Response] =
      service(req).handle(errorHandler)
  }
}

class HttpServer(scheduler: Scheduler) extends ErrorHandling {

  private val logger = Logger.getLogger(this.getClass)

  private val routers = ping :+: add :+: delete :+: status

  private def ping: Router[Response] = Get / "ping" /> Ok("pong")

  private def add: Router[RequestReader[ApiResponse]] = Post / "api" / "add" /> handleAdd

  private def delete: Router[RequestReader[ApiResponse]] = Post / "api" / "delete" /> handleDelete

  private def status: Router[Future[ApiResponse]] = Get / "api" / "status" /> handleStatus

  private def handleAdd: RequestReader[ApiResponse] =
    body.as[AddServersRequest].embedFlatMap {
      addServersRequest =>
        scheduler.onAddServer(addServersRequest)
        Future.value(ApiResponse(success = true, s"Added ${addServersRequest.amount} servers", Some(scheduler.cluster)))
    }

  private def handleDelete: RequestReader[ApiResponse] =
    body.as[DeleteServersRequest].embedFlatMap {
      deleteServerRequest =>
        scheduler.onDeleteServer(deleteServerRequest)
        Future.value(ApiResponse(success = true, s"Deleted servers ${deleteServerRequest.ids.mkString(",")}", Some(scheduler.cluster)))
    }

  private def handleStatus: Future[ApiResponse] =
    Future.value(ApiResponse(success = true, s"Server status", Some(scheduler.cluster)))

  private def makeService: Service[Request, Response] = handleExceptions andThen routers.toService

  var server: ListeningServer = null

  def stop(): Future[Unit] = {
    if (server != null)
      Await.ready(server.close())
    else Future.value(Try(()))
  }

  def start(): Unit = {
    logger.info(s"Starting http server on ${scheduler.config.RestApi}")
    server = Httpx.serve(scheduler.config.RestApi, makeService)
  }
}