package dht.common.request;

public class BadRequest extends Request {
	public BadRequest()
	{
		this.method = "bad";
	}
	@Override
	public void populateParameters() {
		
		
	}
}
