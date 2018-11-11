package dht.server.method;

import dht.common.Context;
import dht.common.request.Request;
import dht.common.response.Response;
import dht.common.response.Response2;

public class KillMethod extends Method {
	public Response2 run(Request req) {
		Context context = Context.getInstance();
		context.stopRunning();
		return new Response2(false, req.method,"OK");
	};

}
