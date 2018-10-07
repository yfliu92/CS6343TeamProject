package dht.common.request;

import javax.json.JsonObject;

abstract public class Request {
	public String from;
	public String to;
	public long epoch;
	public int  id;
	public String method;
	
	public Request() {};
	
	public Request(JsonObject jobj) {
	}
	
	public void fillHeader(JsonObject jobj)
	{
		try {
	        this.to = jobj.getString("to");
	        this.from = jobj.getString("from");
	        this.epoch = jobj.getJsonNumber("epoch").longValue();
	        this.id    = jobj.getJsonNumber("id").intValue();
		} catch (NullPointerException e)
		{
			System.err.println("Request Fill Header Missing Values (null ptr)" + jobj.toString() + e.getLocalizedMessage());
		}
	}
}
