package dht.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import dht.common.Configuration;
import dht.server.RequestMap;
import dht.server.RequestRouter;

public class BaseServer {
	protected Configuration config;
	protected volatile boolean isRunning;
	protected RequestMap map;
	
	public BaseServer(Configuration config)
	{
		this.config = config;
		this.isRunning = true;
		this.map = null;
	}
	
	public void buildRouting() {
		throw new UnsupportedOperationException("Need to override base build routing function for this server.");
	}
	
	public void run()
	{
		if(this.map == null)
		{
			System.err.println("Server's map not configured before calling run;");
			return;
		}
		
		System.out.printf("==== Server with Mode: %s =====\n", this.config.getMode());

		// Adapted from http://cs.lmu.edu/~ray/notes/javanetexamples/
		try (ServerSocket listener = new ServerSocket(this.config.getPort())){
			System.out.println("Binding to: " + this.config.getHost() + ":" + Integer.toString(this.config.getPort()));
			while(true) {
				Socket socket = listener.accept();
				new RequestRouter(socket, map).start();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
