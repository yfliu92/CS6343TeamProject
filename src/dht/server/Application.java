package dht.server;

import dht.server.impl.SingleServer.SingleServer;

public class Application {
	public static void main(String[] args) {
		System.out.println("Opening application");
		
		// TODO: Implement logic for different server implementations
		SingleServer server = new SingleServer("127.0.0.1",8100);
		
		server.BuildRouting();
		server.run();
	}
}
