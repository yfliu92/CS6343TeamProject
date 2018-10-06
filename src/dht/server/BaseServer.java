package dht.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import dht.server.RequestMap;
import dht.server.RequestRouter;

public class BaseServer {
	protected String ipv4;
	protected int port;
	protected volatile boolean isRunning;
	protected RequestMap map;
	
	public BaseServer(String ip, int port)
	{
		this.ipv4 = ip;
		this.port = port;
		this.isRunning = true;
		this.map = null;
	}
	
	public void BuildRouting() {
		throw new UnsupportedOperationException("Need to override base build routing function for this server.");
	}
	
	public void run()
	{
		if(this.map == null)
		{
			System.err.println("Server's map not configured before calling run;");
			return;
		}
		
		System.out.println("==== Base JSON Server =====\n");

		// Adapted from http://cs.lmu.edu/~ray/notes/javanetexamples/
		try (ServerSocket listener = new ServerSocket(this.port)){
			System.out.println("Binding to: " + this.ipv4 + ":" + Integer.toString(this.port));
			while(true) {
				Socket socket = listener.accept();
				new RequestRouter(socket, map).start();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
