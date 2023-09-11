package actors

import akka.dispatch.Mapper
import org.apache.commons.lang3.StringUtils
import org.sunbird.common.JsonUtils
import org.sunbird.common.dto.Request
import org.sunbird.common.dto.Response
import org.sunbird.common.exception.ClientException
import org.sunbird.common.exception.ServerException
import org.sunbird.search.dto.SearchDTO
import org.sunbird.search.processor.SearchProcessor
import org.sunbird.search.util.SearchConstants
import org.sunbird.telemetry.logger.TelemetryManager
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class AuditHistoryActor extends SearchBaseActor {

  override def onReceive(request: Request): Future[Response] = {
    val operation = request.getOperation
    val processor = new SearchProcessor

    if (StringUtils.equalsIgnoreCase("SEARCH_OPERATION_AND", operation)) {
      val searchDTO = getSearchDTO(request)
      TelemetryManager.log(s"Setting search criteria to fetch audit records from ES: $searchDTO")

      val searchResult = processor.processSearch(searchDTO, true)
      searchResult.map { lstResult =>
        val results = lstResult.getOrElse("results", List.empty).asInstanceOf[List[Map[String, Any]]]
        val updatedResults = results.map { result =>
          if (result.contains("logRecord")) {
            val logRecordStr = result("logRecord").asInstanceOf[String]
            if (logRecordStr != null && !logRecordStr.isEmpty) {
              try {
                val logRecord = JsonUtils.deserialize(logRecordStr, classOf[Map[String, Any]])
                result + ("logRecord" -> logRecord)
              } catch {
                case e: Exception =>
                  throw new ServerException("ERR_DATA_PARSER", s"Unable to parse data! | Error is: ${e.getMessage}")
              }
            } else {
              result
            }
          } else {
            result
          }
        }

        OK("audit_history_record", lstResult)
      }
    } else {
      TelemetryManager.log(s"Unsupported operation: $operation")
      Future.failed(new ClientException(SearchConstants.ERR_INVALID_OPERATION, s"Unsupported operation: $operation"))
    }
  }
}
