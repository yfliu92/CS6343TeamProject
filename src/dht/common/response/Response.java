package dht.common.response;

import java.lang.reflect.Field;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

public class Response {

	Boolean status;
	String message;
	
	public Response(boolean issuccess, String message) {
		this.status = issuccess;
		this.message = message;
	}
	
	public JsonObject toJSON() {
		JsonObject jsonObj = Json.createObjectBuilder()
				.add("status",this.status)
				.add("message",this.message)
				.build();
		return jsonObj;
	}
	
	public String serialize() {
		return this.toJSON().toString();
	}
}
