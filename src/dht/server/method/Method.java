package dht.server.method;

import dht.common.request.Request;
import dht.common.response.Response;

abstract public class Method {
	public Response run(Request req) {
		return null;
	};
}
