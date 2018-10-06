package dht.server;

import java.util.HashMap;

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
	
	public void Execute(String method_key, String params)
	{
		Method method = this.map.get(method_key.toLowerCase());
		method.run(params);
	}
}