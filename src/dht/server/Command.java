package dht.server;

public class Command {

	String action;
	String input;
	String node1;
	String node2;
	
	public Command(String action, String input){
		this.action = action;
		this.input = input;
	}
	
	public Command(String action, String node1, String node2){
		this.action = action;
		this.node1 = node1;
		this.node2 = node2;
	}
}
