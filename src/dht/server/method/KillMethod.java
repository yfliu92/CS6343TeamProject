package dht.server.method;

import dht.common.Context;
import dht.common.request.Request;
import dht.common.response.Response;

public class KillMethod extends Method {
	public Response run(Request req) {
		Context context = Context.getInstance();
		context.stopRunning();
		return new Response(false, req.method,"OK");
	};

}
