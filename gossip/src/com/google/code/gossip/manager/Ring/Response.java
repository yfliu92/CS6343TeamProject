package com.google.code.gossip.manager.Ring;

import java.lang.reflect.Field;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

public class Response {

	Boolean status;
	String message = null;
	String result = null;
	JsonObject jsonResult = null;
	
	public Response(boolean issuccess, String result) {
		this.status = issuccess;
		this.result = result;
	}
	
	public Response(boolean issuccess, JsonObject jsonResult) {
		this.status = issuccess;
		this.jsonResult = jsonResult;
	}
	
	public Response(boolean issuccess, JsonObject jsonResult, String message) {
		this.status = issuccess;
		this.jsonResult = jsonResult;
		this.message = message;
	}
	
	public Response(boolean issuccess, String result, String message) {
		this.status = issuccess;
		this.message = message;
		this.result = result;
	}
	
	public JsonObject toJSON() {
		JsonObjectBuilder jsonBuilder = Json.createObjectBuilder();
		jsonBuilder.add("status",this.status);
		if (this.message != null) {
			jsonBuilder.add("message",this.message);
		}
		if (this.result != null) {
			jsonBuilder.add("result",this.result);
		}
		if (this.jsonResult != null) {
			jsonBuilder.add("jsonResult",this.jsonResult);
		}
		return jsonBuilder.build();
	}
	
	public String serialize() {
		return this.toJSON().toString();
	}
}
