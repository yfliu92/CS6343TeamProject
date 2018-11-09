package dht.server.method;


import javax.json.Json;
import com.oath.halodb.HaloDBException;

import dht.common.Configuration;
import dht.common.Context;
import dht.common.FileObject;
import dht.common.repository.Repository;
import dht.common.request.ReadRequest;
import dht.common.request.Request;
import dht.common.response.Response;
import dht.common.response.Response2;

public class ReadBaseMethod extends Method {
	@Override
	public Response2 run(Request req)
	{
		Configuration config = Configuration.getInstance();
		Context context = Context.getInstance();
		Repository repo = Repository.getInstance();
		Response2 res = new Response2(config.getNodeId(), req.from, req.id, "read");
		ReadRequest readReq = (ReadRequest) req;
		
		try {
			FileObject fo = repo.getFile(readReq.key);
			res.params =  Json.createObjectBuilder()
			.add("key",fo.key)
			.add("filename",fo.filename)
			.add("version", fo.version)
			.add("size", fo.size)
			.build();
			
			res.status = "OK";
		} catch (HaloDBException e) {
			System.err.println("ReadBaseMethod.run: Could not find file key:" + Integer.toString(readReq.key));
			res.status = "FAIL";
		}
		
		System.out.println("READ: Params provided were: " + readReq);
				
		// TODO: Write logic to check epoch
		res.epoch = context.getEpoch();
		
		return res;
	}
}
// String from, String to, int id, String method