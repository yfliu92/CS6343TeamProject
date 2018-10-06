package dht.server.method;

public class WriteBaseMethod extends Method {
	@Override
	public void run(String params)
	{
		System.out.println("WRITE: Params provided were: " + params);
	}
}
