package dht.Ring;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;

import dht.server.Command;

public class ProxyServer extends PhysicalNode {

	public ProxyServer(){
		super();
	}
	
	public static String getFindInfo(String input) {
		return input.toUpperCase();
	}
	
	public static String getLoadBalanceResult(String node1, String node2) {
		return "load balance success";
	}
	

	
	public String getResponse(String commandStr) {
		Command command = new Command(commandStr);
		if (command.getAction().equals("find")) {
			return getFindInfo(command.getInput());
		}
		else if (command.getAction().equals("loadbalance")) {
			return getLoadBalanceResult(command.node1, command.node2);
		}
		else if (command.getAction().equals("add")) {
			String ip = command.getCommandSeries().get(0);
			int port = Integer.valueOf(command.getCommandSeries().get(1));
			String result = super.addNode(ip, port);
			return result;
		}
		else if (command.getAction().equals("remove")) {
			int hash = Integer.valueOf(command.getCommandSeries().get(0));
			String result = super.deleteNode(hash);
			return result;
//			return "remove";
		}
		else if (command.getAction().equals("info")) {
			return "info";
		}
		else {
			return "";
		}
	}
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		ProxyServer proxy = new ProxyServer();
    	System.out.println("server running at 9091");
        ServerSocket listener = new ServerSocket(9091);;

        try {
            while (true) {
            	Socket socket = listener.accept();
            	System.out.println("Connection accepted" + " ---- " + new Date().toString());
                try {
                	BufferedReader in = new BufferedReader(
                            new InputStreamReader(socket.getInputStream()));
                    PrintWriter out =
                            new PrintWriter(socket.getOutputStream(), true);
                	String msg;
                	while(true) {
                		msg = in.readLine();
                    	if (msg != null) {
                        	System.out.println("request received: " + msg + " ---- " + new Date().toString());

                            String response = proxy.getResponse(msg);
                            out.println(response);
                            System.out.println("Response sent: " + response);
                    	}
                    	else {
                    		System.out.println("connection end " + " ---- " + new Date().toString());
                    		break;
                    	}
                	}

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
