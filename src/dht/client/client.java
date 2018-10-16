package dht.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Arrays;
import java.util.Date;

import javax.swing.JOptionPane;

public class client {

	public static String getCommandStr(String[] args) {
		if (args == null || args.length == 0) return "";
		StringBuilder command = new StringBuilder();
		for(int i = 0; i < args.length; i++) {
			if (command.length() > 0) {
				command.append(" ");
			}
			command.append(args[i]);
		}
		return command.toString();
	}
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		if (args.length == 0) {
			System.out.println("You need to enter at least one parameter!");
			System.out.println("Enter command 'find XXX' or 'loadbalance XXX'");
			return;
		}

		String serverAddress = "localhost";
        Socket s = new Socket(serverAddress, 9090);
        PrintWriter out =
                new PrintWriter(s.getOutputStream(), true);
            out.println(getCommandStr(args) + " | X");
        BufferedReader input =
            new BufferedReader(new InputStreamReader(s.getInputStream()));
        String answer = input.readLine();
//        JOptionPane.showMessageDialog(null, answer);
        String timeReceived = new Date().toString();
        System.out.println(timeReceived + " -- response Received: " + answer);
        System.exit(0);
		
	}
	


}
