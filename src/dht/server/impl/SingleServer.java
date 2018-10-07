package dht.server.impl;

import dht.common.Configuration;
import dht.server.BaseServer;
import dht.server.RequestMap;
import dht.server.method.*;

public class SingleServer extends BaseServer {
	public SingleServer(Configuration config) {
		super(config);
	}
	
	@Override
	public void buildRouting()
	{
		this.map = new RequestMap();
		this.map.AddMethod("kill", new KillMethod());
		this.map.AddMethod("write", new WriteBaseMethod());
	}
}
