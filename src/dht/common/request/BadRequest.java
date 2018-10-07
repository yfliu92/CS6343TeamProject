package dht.common.request;

import javax.json.JsonObject;

public class BadRequest extends Request {
	public BadRequest()
	{
		this.method = "bad";
	}
	
	@Override
	public void populateParameters(JsonObject params) {
		
		
	}
}
