package dht.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import dht.Ring.*;

public class server {

	public static String getFindInfo(String input) {
		return input.toUpperCase();
	}
	
	public static void addWord(String input) {

	}
	
	public static String getLoadBalanceResult(String node1, String node2) {
		return "success";
	}
	
	public static Command getCommand(String commandStr) {
		
		String[] series = commandStr.split(" ");
		if (series[0].equals("find")) {
			return new Command("find", series[1]);
		}
		else if (series[0].equals("loadbalance")) {
			return new Command("loadbalance", series[1], series[2]);
		}
		else if (series[0].equals("add")) {
			return new Command("add", series[1]);
		}
		else {
			return null;
		}
	}
	
	public static String getResponse(String commandStr) {
		Command command = getCommand(commandStr);
		if (command.action.equals("find")) {
			return getFindInfo(command.input);
		}
		else if (command.action.equals("loadbalance")) {
			return getLoadBalanceResult(command.node1, command.node2);
		}
		else if (command.action.equals("add")) {
			return "added";
		}
		else {
			return "";
		}
	}
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

    	System.out.println("server running at 9090");
        ServerSocket listener = new ServerSocket(9090);
        try {
            while (true) {
                Socket socket = listener.accept();
                try {
                	BufferedReader in = new BufferedReader(
                            new InputStreamReader(socket.getInputStream()));
                	String msg = in.readLine();
                	String timeReceived = new Date().toString();
                	System.out.println(timeReceived + " -- request received: " + msg);
                    PrintWriter out =
                        new PrintWriter(socket.getOutputStream(), true);
                    String response = getResponse(msg);
                    out.println(response);
                    System.out.println("Response: " + response);
                } finally {
                    socket.close();
                }
            }
        }
        finally {
            listener.close();
        }
		
	}

}
