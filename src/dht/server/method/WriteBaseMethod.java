package dht.server.method;


import com.oath.halodb.HaloDBException;

import dht.common.Configuration;
import dht.common.Context;
import dht.common.FileObject;
import dht.common.repository.Repository;
import dht.common.request.Request;
import dht.common.request.WriteRequest;
import dht.common.response.Response;

public class WriteBaseMethod extends Method {
	@Override
	public Response run(Request req)
	{
		Configuration config = Configuration.getInstance();
		Context context = Context.getInstance();
		Repository repo = Repository.getInstance();
		Response res = new Response(config.getNodeId(), req.from, req.id, "write");
		
		
		FileObject fo = ((WriteRequest) req).getFileObject();
		try {
			repo.saveFile(fo);
			res.status = "OK";
		} catch (HaloDBException e) {
			// TODO Auto-generated catch block
			res.status = "FAIL";
		}
		
		System.out.println("WRITE: Params provided were: " + req);
				
		// TODO: Write logic to check epoch
		res.epoch = context.getEpoch();
		
		return res;
	}
}
// String from, String to, int id, String method