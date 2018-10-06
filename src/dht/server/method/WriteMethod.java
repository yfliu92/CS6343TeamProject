package dht.server.method;

public class WriteMethod extends Method {
	@Override
	public void run(String params)
	{
		System.out.println("WRITE: Params provided were: " + params);
	}
}
