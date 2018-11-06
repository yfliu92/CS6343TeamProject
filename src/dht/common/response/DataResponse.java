package dht.common.response;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import storage_server.Datum;

public class DataResponse extends Response{
	
	Datum result;
	JsonObject objArray;

	public DataResponse(boolean issuccess, String message) {
		super(issuccess, message);
	}
	
	public DataResponse(boolean issuccess, String message, Datum datum) {
		super(issuccess, message);
		this.result = datum;
	}
	
	public DataResponse(boolean issuccess, String message, JsonObject objArray) {
		super(issuccess, message);
		this.objArray = objArray;
	}
	
	public JsonObject toJSON() {
		JsonObjectBuilder jsonBuilder = Json.createObjectBuilder();
		jsonBuilder.add("status",super.status)
		.add("message",super.message);
		if (this.result != null) {
			jsonBuilder.add("result",this.result.toJSON());
		}
		else if (this.objArray != null) {
			jsonBuilder.add("result",this.objArray);
		}
		
		JsonObject jsonObj = jsonBuilder.build();		
		return jsonObj;
	}
	
	public String serialize() {	
		return this.toJSON().toString();
	}
}
