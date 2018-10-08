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
            	case "kill":
            		req = new KillRequest(jobj);
            		break;
            	case "write":
            		req = new WriteRequest(jobj);
            		break;
            	case "read":
            		req = new ReadRequest(jobj);
            		break;
        		default:
        			req = new BadRequest(jobj);
        			break;
            }
            
        } catch (JsonException e ) {
        	System.err.println("Unable to parse request " + input + e.getLocalizedMessage());
        	req = null;
        } catch (NullPointerException e ) {
        	System.err.println("Request Reader Null Pointer Exception " + input + e.getLocalizedMessage());
        	req = null;
        }
		return req;
	}
}
