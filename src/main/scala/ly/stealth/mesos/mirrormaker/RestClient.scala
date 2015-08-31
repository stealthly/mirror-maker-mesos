package ly.stealth.mesos.mirrormaker

import java.net.URL

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.twitter.finagle.httpx.Version.Http11
import com.twitter.finagle.{httpx, Httpx}
import com.twitter.finagle.httpx.Method
import com.twitter.io.Buf.ByteArray.Owned
import com.twitter.io.Reader
import com.twitter.util.Future

/**
 * REST client to the HttpServer - scheduler rest api for getting status,
 * adding, removing tasks. Client, once created, is bound to one server -
 * its location specified by 'baseUrl' parameter
 *
 * @param baseUrl MMM Scheduler Http Server address. E.g. http://192.168.3.5:7000/api
 */
class RestClient(baseUrl: String) {

  private val StatusPath = "status"
  private val AddPath = "add"
  private val DeletePath = "delete"

  implicit val objectMapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  private val (authority, file, host) = extractAuthorityFileHost(baseUrl)

  val client = Httpx.newService(authority)

  private def extractAuthorityFileHost(urlStr: String): (String, String, String) = {
    val url = new URL(urlStr)
    (url.getAuthority, url.getFile, url.getHost)
  }

  /**
   * Generic method for executing requests to the REST API server
   * @param method Http Method
   * @param path path identifying resource
   * @param dataOpt optionally [[com.twitter.io.Reader]] if request has to input body
   * @param responseClass [[java.lang.Class]] instance of the unmarshalled response
   * @tparam Req type of the request
   * @tparam Res type of the unmarshalled response
   * @return unmarshalled response of the REST call
   */
  private def doCall[Req, Res](method: Method, path: String, dataOpt: Option[Req], responseClass: Class[Res]): Future[Res] = {
    val request =
      dataOpt match {
        case Some(data) =>
          val body = Reader.fromBuf(Owned(objectMapper.writeValueAsBytes(data)))
          httpx.Request(Http11, method, file + "/" + path, body)
        case None =>
          httpx.Request(Http11, method, file + "/" + path)
      }

    request.host = host
    val response = client(request)

    response.map(r => objectMapper.readValue(r.contentString, responseClass))
  }

  def status(): Future[ApiResponse] = {
    doCall(Method.Get, StatusPath, None, classOf[ApiResponse])
  }

  def add(addServersRequest: AddServersRequest): Future[ApiResponse] = {
    doCall(Method.Post, AddPath, Some(addServersRequest), classOf[ApiResponse])
  }

  def delete(deleteServersRequest: DeleteServersRequest): Future[ApiResponse] = {
    doCall(Method.Post, DeletePath, Some(deleteServersRequest), classOf[ApiResponse])
  }
}