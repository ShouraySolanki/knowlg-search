package org.sunbird.content.actors

import akka.actor.Props
import org.scalamock.scalatest.MockFactory
import org.sunbird.cloudstore.StorageService
import org.sunbird.common.dto.Request
import org.sunbird.graph.dac.model.Node
import org.sunbird.graph.{GraphService, OntologyEngineContext}

import scala.concurrent.ExecutionContext.Implicits.global

import java.util
import scala.collection.JavaConverters._
import scala.concurrent.Future

class TestAppActor extends BaseSpec with MockFactory {

  "AppActor" should "return failed response for 'unknown' operation" in {
    implicit val ss = mock[StorageService]
    implicit val oec: OntologyEngineContext = new OntologyEngineContext
    testUnknownOperation(Props(new AppActor()), getRequest())
  }

  it should "return success response for 'create' operation" in {
    implicit val oec: OntologyEngineContext = mock[OntologyEngineContext]
    val graphDB = mock[GraphService]
    (oec.graphService _).expects().returns(graphDB)
    val node = new Node("domain", "DATA_NODE", "App")
    node.setIdentifier("android:org.test.sunbird.integration")
    node.setObjectType("App")
    (graphDB.addNode(_: String, _: Node)).expects(*, *).returns(Future(node))
    val request = getRequest()
    request.getRequest.put("name", "Test Integration App")
    request.getRequest.put("description", "Description of Test Integration App")
    request.getRequest.put("provider", Map("name" -> "Test Organisation", "copyright" -> "CC BY 4.0").asJava)
    request.getRequest.put("osType", "android")
    request.getRequest.put("osMetadata", Map("packageId" -> "org.test.integration", "appVersion" -> "1.0", "compatibilityVer" -> "1.0").asJava)
    request.getRequest.put("appTarget", Map("mimeType" -> util.Arrays.asList()).asJava)
    request.setOperation("create")
    val response = callActor(request, Props(new AppActor()))
    assert("successful".equals(response.getParams.getStatus))
  }

  private def getRequest(): Request = {
    val request = new Request()
    request.setContext(new util.HashMap[String, AnyRef]() {
      {
        put("graph_id", "domain")
        put("version", "1.0")
        put("objectType", "App")
        put("schemaName", "app")
        put("X-Channel-Id", "org.sunbird")
      }
    })
    request.setObjectType("App")
    request
  }

}
