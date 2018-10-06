package dht.server.impl.SingleServer;

import dht.server.BaseServer;
import dht.server.RequestMap;
import dht.server.method.*;

public class SingleServer extends BaseServer {
	public SingleServer(String ip, int port) {
		super(ip, port);
	}
	
	@Override
	public void BuildRouting()
	{
		this.map = new RequestMap();
		this.map.AddMethod("write", new WriteBaseMethod());
	}
}
