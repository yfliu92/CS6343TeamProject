package dht.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import dht.server.RequestMap;
import dht.server.RequestRouter;

public class BaseServer {
	String ipv4;
	int port;
	volatile boolean isRunning;
	
	public BaseServer(String ip, int port)
	{
		this.ipv4 = ip;
		this.port = port;
		this.isRunning = true;
	}
	
	public void run()
	{
		RequestMap map = new RequestMap();
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
