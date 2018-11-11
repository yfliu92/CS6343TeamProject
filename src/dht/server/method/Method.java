package dht.server.method;

import dht.common.request.Request;
import dht.common.response.Response;
import dht.common.response.Response2;

abstract public class Method {
	public Response2 run(Request req) {
		return null;
	};
}
