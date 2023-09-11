package actors

import akka.actor.ActorSystem
import akka.pattern.Patterns
import akka.util.Timeout
import org.sunbird.actor.core.BaseActor
import org.sunbird.common.Platform
import org.sunbird.common.dto.Request
import org.sunbird.common.dto.Response
import org.sunbird.common.dto.ResponseParams
import org.sunbird.common.exception.ResponseCode
import org.sunbird.search.client.ElasticSearchUtil
import org.sunbird.search.util.SearchConstants
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class HealthActor extends BaseActor {
  implicit val timeout: Timeout = Timeout(30.seconds)
  val actorSystem: ActorSystem = ActorSystem.create("HealthActorSystem")

  override def onReceive(request: Request): Future[Response] = {
    ElasticSearchUtil.initialiseESClient(SearchConstants.COMPOSITE_SEARCH_INDEX, Platform.config.getString("search.es_conn_info"))
    checkIndexHealth()
  }

  private def checkIndexHealth(): Future[Response] = {
    val indexExists: Boolean = ElasticSearchUtil.isIndexExists(SearchConstants.COMPOSITE_SEARCH_INDEX)

    if (indexExists) {
      val responseParams = new ResponseParams()
      responseParams.setErr("0")
      responseParams.setStatus(ResponseParams.StatusType.successful.toString)
      responseParams.setErrmsg("Operation successful")

      val responseData = Map("name" -> "ElasticSearch", "healthy" -> true)
      val response = new Response(responseParams, responseData)
      Future.successful(response)
    } else {
      val responseParams = new ResponseParams()
      responseParams.setStatus(ResponseParams.StatusType.failed.toString)
      responseParams.setErrmsg("Elastic Search index is not available")

      val responseData = Map(
        "name" -> "ElasticSearch",
        "healthy" -> false,
        "err" -> "404",
        "errmsg" -> "Elastic Search index is not available"
      )

      val response = new Response(ResponseCode.SERVER_ERROR, responseParams, responseData)
      Future.successful(response)
    }
  }
}
