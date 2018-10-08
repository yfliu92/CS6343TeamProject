package dht.common.response;

import javax.json.JsonException;
import javax.json.JsonObject;

public class ReadResponse extends Response {
	public int key;
	public String filename;
	public int version;
	public int size;
	
	public ReadResponse(JsonObject jobj)
	{
		super("read");
		try {
			this.fillHeader(jobj);
			JsonObject params = jobj.getJsonObject("parameters");
			this.key = params.getJsonNumber("key").intValue();
			this.filename = params.getString("filename");
			this.version = params.getJsonNumber("version").intValue();
			this.size    = params.getJsonNumber("size").intValue();
		} catch (JsonException e ) {
	        	System.err.println("Read Response unable to parse read response " + jobj.toString() + e.getLocalizedMessage());
        } catch (NullPointerException e ) {
	        	System.err.println("Read Response null pointer exception" +  jobj.toString()  + e.getMessage());
	    }
	}
	
	public ReadResponse(String from, String to, int id, String method) {
		super(from, to, id, method);

	}

}
