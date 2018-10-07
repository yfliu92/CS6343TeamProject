package dht.server.method;

import dht.common.request.Request;

public class WriteBaseMethod extends Method {
	@Override
	public void run(Request req)
	{
		
		System.out.println("WRITE: Params provided were: " + req);
		
	}
}
