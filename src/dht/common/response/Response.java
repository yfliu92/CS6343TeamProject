package dht.common.response;

import java.io.ByteArrayOutputStream;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonWriter;

public class Response {
	public String from;
	public String to;
	public long epoch;
	public int  id;
	public String method;
	public String status;
	public String message;
	public String rtable;
	public boolean needs_rtable;
	
	public Response(String from, String to, int id, String method)
	{
		this.from = from;
		this.to = to;
		this.id = id;
		this.method = method;
		this.needs_rtable = false;
	}
	
	public ByteArrayOutputStream toByteStream()
	{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		JsonWriter writer = Json.createWriter(baos);
		JsonObjectBuilder jobjb = Json.createObjectBuilder()
				.add("from",this.from)
				.add("to",this.to)
				.add("epoch",this.epoch)
				.add("id",this.id)
				.add("method",this.method)
				.add("status",this.status);
		
		if(this.message != null && this.message != "")
			jobjb.add("message",this.message);
		
		if(needs_rtable)
			jobjb.add("rtable",this.rtable);
		
		JsonObject jobj = jobjb.build();
		writer.writeObject(jobj);
		writer.close();
		return baos;
	}
}
