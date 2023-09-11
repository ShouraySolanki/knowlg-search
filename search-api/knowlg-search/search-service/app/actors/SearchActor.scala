package actors

import akka.dispatch.Futures
import akka.dispatch.Mapper
import akka.dispatch.Recover
import akka.util.Timeout
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.collections.MapUtils
import org.apache.commons.lang3.StringUtils
import org.sunbird.common.JsonUtils
import org.sunbird.common.Platform
import org.sunbird.common.dto.Request
import org.sunbird.common.dto.Response
import org.sunbird.common.exception.ClientException
import org.sunbird.common.exception.ResponseCode
import org.sunbird.search.dto.SearchDTO
import org.sunbird.search.processor.SearchProcessor
import org.sunbird.search.util.DefinitionUtil
import org.sunbird.search.util.SearchConstants
import org.sunbird.telemetry.logger.TelemetryManager
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._
import scala.collection.mutable.{ListBuffer, Map => MutableMap}
import java.util.{Map => JavaMap, List => JavaList}

class SearchActor extends SearchBaseActor {
  private val WAIT_TIMEOUT = Timeout(Duration.create(30000, "milliseconds"))

  override def receive: Receive = {
    case request: Request =>
      val operation = request.getOperation
      val processor = new SearchProcessor()

      val resultFuture = try {
        if (StringUtils.equalsIgnoreCase("INDEX_SEARCH", operation)) {
          val searchDTO = getSearchDTO(request)
          val searchResult: Future[Map[String, Object]] = processor.processSearch(searchDTO, true)

          searchResult.flatMap { lstResult =>
            val mode = Option(request.getRequest.get(SearchConstants.mode)).getOrElse("").toString
            if (StringUtils.isNotBlank(mode) && StringUtils.equalsIgnoreCase("collection", mode)) {
              Future.successful(OK(getCollectionsResult(lstResult, processor, request)))
            } else {
              Future.successful(OK(lstResult))
            }
          }.recoverWith {
            case failure: Throwable =>
              TelemetryManager.error("Unable to process the request:: Request: " + JsonUtils.serialize(request), failure)
              Future.successful(ERROR(request.getOperation, failure))
          }
        } else if (StringUtils.equalsIgnoreCase("COUNT", operation)) {
          val countResult = processor.processCount(getSearchDTO(request))
          countResult.get("count") match {
            case count: Integer =>
              Future.successful(OK("count", count))
            case _ =>
              Future.successful(ERROR("", "count is empty or null", ResponseCode.SERVER_ERROR, "", null))
          }
        } else if (StringUtils.equalsIgnoreCase("METRICS", operation)) {
          val searchResult: Future[JavaMap[String, Object]] = processor.processSearch(getSearchDTO(request), false)
          searchResult.map { lstResult =>
            OK(getCompositeSearchResponse(lstResult))
          }
        } else if (StringUtils.equalsIgnoreCase("GROUP_SEARCH_RESULT_BY_OBJECTTYPE", operation)) {
          val searchResponse = request.get("searchResult").asInstanceOf[JavaMap[String, Object]]
          Future.successful(OK(getCompositeSearchResponse(searchResponse)))
        } else {
          TelemetryManager.log("Unsupported operation: " + operation)
          throw new ClientException(SearchConstants.ERR_INVALID_OPERATION, "Unsupported operation: " + operation)
        }
      } catch {
        case e: Exception =>
          TelemetryManager.info("Error while processing the request: REQUEST::" + JsonUtils.serialize(request))
          Future.successful(ERROR(operation, e))
      }

  }

  private def isEmpty(o: Any): Boolean = {
    o match {
      case s: String => StringUtils.isBlank(s)
      case list: List[_] => list.isEmpty
      case arr: Array[_] => arr.isEmpty
      case _ => false
    }
  }

  private def getCollectionsResult(lstResult: Map[String, Object], processor: SearchProcessor, parentRequest: Request): Map[String, Object] = {
    val contentResults = lstResult.getOrElse("results", List.empty[Map[String, Object]]).asInstanceOf[List[Map[String, Object]]]
    if (contentResults.nonEmpty) {
      try {
        val contentIds = contentResults.map(_.getOrElse("identifier", "").toString)
        val request = new Request(parentRequest)
        val filters = Map(
          SearchConstants.objectType -> List("Content", "Collection", "Asset"),
          "mimeType" -> List("application/vnd.ekstep.content-collection"),
          "childNodes" -> contentIds
        )
        request.put(SearchConstants.sort_by, parentRequest.get(SearchConstants.sort_by))
        request.put(SearchConstants.fields, getCollectionFields(
          Option(parentRequest.get(SearchConstants.fields)).map(_.asInstanceOf[java.util.List[String]].asScala.toList).getOrElse(List.empty[String])
        ))
        request.put(SearchConstants.filters, filters)
        val searchDTO = getSearchDTO(request)
        val collectionResult = Await.result(processor.processSearch(searchDTO, true), WAIT_TIMEOUT.duration).asInstanceOf[Map[String, Object]]
        lstResult ++ collectionResult
      } catch {
        case e: Exception =>
          TelemetryManager.error("Error while fetching the collection for the contents : ", e)
          lstResult
      }
    } else {
      lstResult
    }
  }

  private def getCollectionFields(fieldList: List[String]): List[String] = {
    val fields = if (Platform.config.hasPath("search.fields.mode_collection")) {
      Platform.config.getStringList("search.fields.mode_collection").asScala.toList
    } else {
      List("identifier", "name", "objectType", "contentType", "mimeType", "size", "childNodes")
    }

    val mergedFields = if (fieldList.nonEmpty) {
      fields ++ fieldList.distinct
    } else {
      fields
    }

    mergedFields.distinct
  }

  def prepareCollectionResult(collectionResult: MutableMap[String, Any], contentIds: List[String]): MutableMap[String, Any] = {
    val results = ListBuffer[MutableMap[String, Any]]()
    for (collection <- collectionResult("results").asInstanceOf[List[MutableMap[String, Any]]]) {
      var childNodes = collection.getOrElse("childNodes", List.empty[String]).asInstanceOf[List[String]]
      childNodes = childNodes.intersect(contentIds)
      collection("childNodes") = childNodes
      results += collection
    }
    collectionResult("collections") = results.toList
    collectionResult("collectionsCount") = collectionResult.getOrElse("count", 0)
    collectionResult.remove("count")
    collectionResult.remove("results")
    collectionResult
  }
}
