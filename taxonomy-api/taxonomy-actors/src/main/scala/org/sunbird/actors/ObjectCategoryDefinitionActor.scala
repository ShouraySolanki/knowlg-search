package org.sunbird.actors

import java.util

import javax.inject.Inject
import org.apache.commons.lang3.StringUtils
import org.sunbird.actor.core.BaseActor
import org.sunbird.common.Slug
import org.sunbird.common.dto.{Request, Response, ResponseHandler}
import org.sunbird.common.exception.ClientException
import org.sunbird.graph.OntologyEngineContext
import org.sunbird.graph.nodes.DataNode
import org.sunbird.graph.utils.NodeUtil
import org.sunbird.utils.{Constants, RequestUtil}

import scala.collection.JavaConverters
import scala.concurrent.{ExecutionContext, Future}

class ObjectCategoryDefinitionActor @Inject()(implicit oec: OntologyEngineContext) extends BaseActor {

	implicit val ec: ExecutionContext = getContext().dispatcher

	override def onReceive(request: Request): Future[Response] = {
		request.getOperation match {
			case Constants.CREATE_OBJECT_CATEGORY_DEFINITION => create(request)
			case Constants.READ_OBJECT_CATEGORY_DEFINITION => read(request)
			case Constants.UPDATE_OBJECT_CATEGORY_DEFINITION => update(request)
			case _ => ERROR(request.getOperation)
		}
	}

	private def create(request: Request): Future[Response] = {
		RequestUtil.restrictProperties(request)
		val categoryId = request.getRequest.getOrDefault(Constants.CATEGORY_ID, "").asInstanceOf[String]
		val channelId = request.getRequest.getOrDefault(Constants.CHANNEL, "all").asInstanceOf[String]
		val targetObjectType = request.getRequest.getOrDefault(Constants.TARGET_OBJECT_TYPE, "").asInstanceOf[String]
		if (StringUtils.isNotBlank(categoryId) && (StringUtils.isNotBlank(targetObjectType) && StringUtils.isNotBlank(channelId))) {
			val identifier = categoryId + "_" + Slug.makeSlug(targetObjectType) + "_" + Slug.makeSlug(channelId)
			request.put(Constants.IDENTIFIER, identifier)
			val getCategoryReq = new Request()
			getCategoryReq.setContext(request.getContext)
			getCategoryReq.getContext.put(Constants.SCHEMA_NAME, Constants.OBJECT_CATEGORY_SCHEMA_NAME)
			getCategoryReq.getContext.put(Constants.VERSION, Constants.OBJECT_CATEGORY_SCHEMA_VERSION)
			getCategoryReq.put(Constants.IDENTIFIER, categoryId)
			getCategoryReq.put("fields", new util.ArrayList[String])
			DataNode.read(getCategoryReq).map(node => {
				if (null != node && StringUtils.equalsAnyIgnoreCase(node.getIdentifier, categoryId)) {
					DataNode.create(request).map(node => {
						val response = ResponseHandler.OK
						response.put(Constants.IDENTIFIER, node.getIdentifier)
						response
					})
				} else throw new ClientException("ERR_INVALID_CATEGORY_ID", "Please provide valid category identifier")
			}).flatMap(f => f)
		} else throw new ClientException("ERR_INVALID_REQUEST", "Invalid Request. Please Provide Required Properties!")
	}

	private def read(request: Request): Future[Response] = {
		val fields: util.List[String] = JavaConverters.seqAsJavaListConverter(request.get(Constants.FIELDS).asInstanceOf[String].split(",").filter(field => StringUtils.isNotBlank(field) && !StringUtils.equalsIgnoreCase(field, "null"))).asJava
		request.getRequest.put(Constants.FIELDS, fields)
		DataNode.read(request).map(node => {
			val metadata: util.Map[String, AnyRef] = NodeUtil.serialize(node, fields, request.getContext.get(Constants.SCHEMA_NAME).asInstanceOf[String], request.getContext.get(Constants.VERSION).asInstanceOf[String])
			val response: Response = ResponseHandler.OK
			response.put(Constants.OBJECT_CATEGORY_DEFINITION, metadata)
			response
		})
	}

	private def update(request: Request): Future[Response] = {
		RequestUtil.restrictProperties(request)
		DataNode.update(request).map(node => {
			val response: Response = ResponseHandler.OK
			response.put(Constants.IDENTIFIER, node.getIdentifier)
			response
		})
	}

}
