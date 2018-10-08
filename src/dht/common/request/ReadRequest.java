package dht.common.request;

import javax.json.JsonException;
import javax.json.JsonObject;

import dht.common.FileObject;

public class ReadRequest extends Request {
	public int key;
	
	public ReadRequest(JsonObject jobj)
	{
		try {
			this.method = "read";
			this.fillHeader(jobj);
			JsonObject params = jobj.getJsonObject("parameters");
			this.key = params.getJsonNumber("key").intValue();
		} catch (JsonException e ) {
	        	System.err.println("Read Request unable to parse write request " + jobj.toString() + e.getLocalizedMessage());
        } catch (NullPointerException e ) {
	        	System.err.println("Read Request null pointer exception" +  jobj.toString()  + e.getMessage());
	    }
	}

	@Override
	public String toString()
	{
		return new StringBuilder().append(this.from).append(" ")
				.append(this.to).append(" ")
				.append(this.method).append(" ")
				.append(this.epoch).append(" ")
				.append(this.id).append(" ")
				.append(this.key).append(" ")
				.toString();
	}
}
