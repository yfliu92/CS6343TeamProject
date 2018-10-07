package dht.common.request;

import javax.json.JsonObject;

public class KillRequest extends Request {
	public KillRequest(JsonObject jobj)
	{
		this.method = "kill";
	}
}
