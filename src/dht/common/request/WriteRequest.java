package dht.common.request;

import javax.json.JsonException;
import javax.json.JsonObject;

import dht.common.FileObject;

public class WriteRequest extends Request {
	public int key;
	public String filename;
	public int version;
	public int size;
	
	public WriteRequest(JsonObject jobj)
	{
		try {
			this.method = "write";
			this.fillHeader(jobj);
			JsonObject params = jobj.getJsonObject("parameters");
			this.key = params.getJsonNumber("key").intValue();
			this.filename = params.getString("filename");
			this.version = params.getJsonNumber("version").intValue();
			this.size    = params.getJsonNumber("size").intValue();
		} catch (JsonException e ) {
	        	System.err.println("Write Request unable to parse write request " + jobj.toString() + e.getLocalizedMessage());
        } catch (NullPointerException e ) {
	        	System.err.println("Write Request null pointer exception" +  jobj.toString()  + e.getMessage());
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
				.append(this.filename).append(" ")
				.append(this.version).append(" ")
				.append(this.size).append(" ")
				.toString();
	}
	
	public FileObject getFileObject()
	{
		return new FileObject(
					this.filename,
					this.version,
					this.size,
					System.currentTimeMillis()
				);
	}
}
