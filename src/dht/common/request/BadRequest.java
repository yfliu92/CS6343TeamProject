package dht.common.request;

import javax.json.JsonObject;

public class BadRequest extends Request {
	public BadRequest(JsonObject jobj)
	{
		this.method = "bad";
	}
}
