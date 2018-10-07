package dht.server.method;


import dht.common.Configuration;
import dht.common.Context;
import dht.common.request.Request;
import dht.common.response.Response;

public class WriteBaseMethod extends Method {
	@Override
	public Response run(Request req)
	{
		Configuration config = Configuration.getInstance();
		Context context = Context.getInstance();
		
		Response res = new Response(config.getNodeId(), req.from, req.id, "write");
		System.out.println("WRITE: Params provided were: " + req);
		
		// TODO: Write logic to actually store the file
		res.status = "OK";
		
		// TODO: Write logic to check epoch
		res.epoch = context.getEpoch();
		
		return res;
	}
}
// String from, String to, int id, String method