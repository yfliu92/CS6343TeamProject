package dht.common.request;

import javax.json.JsonObject;

public class WriteRequest extends Request {
	public int key;
	public String filename;
	public int version;
	public int size;
	
	public WriteRequest()
	{
		this.method = "write";
	}

	@Override
	public void populateParameters(JsonObject jobj) {
		this.key = jobj.getJsonNumber("key").intValue();
		this.filename = jobj.getString("filename");
		this.version = jobj.getJsonNumber("version").intValue();
		this.size    = jobj.getJsonNumber("size").intValue();
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
}
