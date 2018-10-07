package dht.common.request;

import javax.json.JsonObject;

abstract public class Request {
	public String from;
	public String to;
	public long epoch;
	public int  id;
	public String method;
	
	abstract public void populateParameters(JsonObject jobj);
}
