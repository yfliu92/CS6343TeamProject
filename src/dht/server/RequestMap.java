package dht.server;

import java.util.HashMap;

import dht.common.request.Request;
import dht.common.response.Response;
import dht.server.method.Method;

public class RequestMap {
	public HashMap<String, Method> map;

	public RequestMap() {
		this.map = new HashMap<>();
	}
	
	public void AddMethod(String method_key, Method method)
	{
		this.map.put(method_key, method);
	}
	
	public Response Execute(Request req) throws NoSuchMethodException
	{
		if (req.method == "bad")
			throw new NoSuchMethodException("Response Execute: Check if RequestReader.readRequest needs new case statement for new request type");
		Method method = this.map.get(req.method);
		if(method == null)
			throw new NoSuchMethodException("Response Execute: Unable to find method "+ req.method + "check if RequestReader.readRequest needs new case statement for new request type");
		
		return method.run(req);
	}
}