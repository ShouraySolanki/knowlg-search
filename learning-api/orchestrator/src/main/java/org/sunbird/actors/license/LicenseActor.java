package org.sunbird.actors.license;

import akka.dispatch.Futures;
import akka.dispatch.Mapper;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.common.dto.Request;
import org.sunbird.common.dto.Response;
import org.sunbird.common.dto.ResponseHandler;
import org.sunbird.common.dto.ResponseParams;
import org.sunbird.graph.dac.model.Node;
import org.sunbird.graph.nodes.DataNode;
import scala.concurrent.Future;
import utils.LicenseOperations;

import java.util.HashMap;

public class LicenseActor extends BaseActor {

    public Future<Response> onReceive(Request request) throws Throwable {
        String operation = request.getOperation();
        if (LicenseOperations.createLicense.name().equals(operation)) {
            return create(request);
        } else if (LicenseOperations.readLicense.name().equals(operation)) {
            return read(request);
        } else if (LicenseOperations.updateLicense.name().equals(operation)) {
            return update(request);
        } else if (LicenseOperations.retireLicense.name().equals(operation)) {
            return retire(request);
        } else {
            return ERROR(operation);

        }
    }

    private Future<Response> create(Request request) throws Exception {
        return DataNode.create(request, getContext().dispatcher())
                .map(new Mapper<Node, Response>() {
                    @Override
                    public Response apply(Node node) {
                        Response response = ResponseHandler.OK();
                        response.put("node_id", node.getIdentifier());
                        response.put("versionKey", node.getMetadata().get("versionKey"));
                        return response;
                    }
                }, getContext().dispatcher());
    }

    private Future<Response> read(Request request) throws Exception {
        ResponseParams responseParams = new ResponseParams();
        responseParams.setResmsgid("b7430a32-b055-438c-b209-c81d37558979");
        responseParams.setMsgid(null);
        responseParams.setErr(null);
        responseParams.setStatus("successful");
        responseParams.setErrmsg(null);
        Response response = new Response();
        response.setParams(responseParams);
        response.put("license", new HashMap<String, Object>() {{
            put("identifier",request.get("identifier"));
            put("name", "TestObject");
            put("description","TestDesc");
            put("url","www.url.com");
            put("code","TestObject");
            put("status","Live");
        }});
        return Futures.successful(response);
    }
    private Future<Response> update(Request request) throws Exception {
        request.getRequest().put("status", "Live");
        return DataNode.update(request, getContext().dispatcher())
                .map(new Mapper<Node, Response>() {
                    @Override
                    public Response apply(Node node) {
                        Response response = ResponseHandler.OK();
                        response.put("node_id", node.getIdentifier());
                        return response;
                    }
                }, getContext().dispatcher());
    }
    private Future<Response> retire(Request request) throws Exception {
        request.getRequest().put("status", "Retired");
        return DataNode.update(request, getContext().dispatcher())
                .map(new Mapper<Node, Response>() {
                    @Override
                    public Response apply(Node node) {
                        Response response = ResponseHandler.OK();
                        response.put("node_id", node.getIdentifier());
                        return response;
                    }
                }, getContext().dispatcher());
    }
}
