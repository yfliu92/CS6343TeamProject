package dht.common.request;

import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonReader;

public class RequestReader {
	public static Request readRequest(String input)
	{
		Request req;
        JsonReader jsonReader = Json.createReader(new StringReader(input));
        try {     	
        	JsonObject jobj = jsonReader.readObject();
            // Parse request
			// Adapted from https://javaee.github.io/jsonp/getting-started.html
        	String method = jobj.getString("method").toLowerCase().trim();
            switch(method) {
            	case "write":
            		req = new WriteRequest();
            		break;
        		default:
        			req = new BadRequest();
        			break;
            }
            
            req.to = jobj.getString("to");
            req.from = jobj.getString("from");
            req.epoch = jobj.getJsonNumber("epoch").longValue();
            req.id    = jobj.getJsonNumber("id").intValue();
            req.populateParameters(jobj.getJsonObject("parameters"));
            
        } catch (JsonException e ) {
        	System.err.println("Unable to parse request " + input + e.getLocalizedMessage());
        	req = null;
        } catch (NullPointerException e ) {
        	System.err.println("Unable to find all required header fields" + input + e.getLocalizedMessage());
        	req = null;
        }
		return req;
	}
}
