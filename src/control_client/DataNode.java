package control_client;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.Console;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.Date;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonWriter;

import dht.server.Command;

public class DataNode {

    PrintWriter output;
    BufferedReader input;
    InputStream inputStream;
    OutputStream outputStream;
	SocketAddress socketAddress;
	Socket socket;
	
    public boolean connectServer(String serverAddress, int port) {
//    	control_client client = new control_client();
    	int timeout = 2000;
		try {
			socketAddress = new InetSocketAddress(serverAddress, port);
			socket = new Socket();
			socket.connect(socketAddress, timeout);
			inputStream = socket.getInputStream();
			outputStream = socket.getOutputStream();
			output = new PrintWriter(outputStream, true);
			input = new BufferedReader(new InputStreamReader(inputStream));
//	        out = new PrintWriter(socket.getOutputStream(), true);
//	        input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			
			String serverport = input.readLine();
	        
	        System.out.println("Connected to server " + serverAddress + ":" + port + ", allocated server port " + serverport);
//			socket.close();
			return true;
 
		} catch (SocketTimeoutException exception) {
//			socket.close();
			System.out.println("SocketTimeoutException " + serverAddress + ":" + port + ". " + exception.getMessage());
			return false;
		} catch (IOException exception) {
//			socket.close();
			System.out.println(
					"IOException - Unable to connect to " + serverAddress + ":" + port + ". " + exception.getMessage());
			return false;
		}
    }
    
    public void sendCommandStr(String command, BufferedReader input, PrintWriter output) throws Exception {
    	String[] jsonCommands = {"read", "write", "data", "dht", "info"};
    	for(String jsonCommand: jsonCommands) {
    		if (command.startsWith(jsonCommand)) {
    			sendCommandStrNew(command, input, output);
    			return;
    		}
    	}

    	System.out.println("Sending command" + " ---- " + new Date().toString());
            output.println(command);
        String response = input.readLine();
        System.out.println("Response received: " + response + " ---- " + new Date().toString());
    }
    
    public void sendCommandStrNew(String command, BufferedReader input, PrintWriter output) throws Exception {
    	
    	String timeStamp = new Date().toString();
    	System.out.println("Sending command" + " ---- " + timeStamp);
        output.println(command);
        
        JsonObject res = parseRequest(input);
        if (res != null) {
            System.out.println();
        	System.out.println("Response received at " + timeStamp + " ---- " + res.toString());
        	System.out.println();
         }
    }
	
    public static JsonObject parseRequest(BufferedReader br) throws Exception {
        String str;
        JsonObject jsonObject = null;

        while ((str = br.readLine()) != null) {
            JsonReader jsonReader = Json.createReader(new StringReader(str));
            jsonObject = jsonReader.readObject();
            return jsonObject;
        }
        return jsonObject;
    }
    
    public void processCommandRush(String cmd) throws Exception {
    	Command command = new Command(cmd);
    	
    	String timeStamp = new Date().toString();
    	System.out.println("Sending command" + " ---- " + timeStamp);
    	System.out.println();
        
        JsonObject params = null;
        JsonObject jobj = null;
		if(command.getAction().equals("addnode")) {
			  params = Json.createObjectBuilder()
			  .add("subClusterId", command.getCommandSeries().get(0))
			  .add("ip", command.getCommandSeries().get(1))
			  .add("port", command.getCommandSeries().get(2))
			  .add("weight", command.getCommandSeries().get(3))
			  .build();
			
			  jobj = Json.createObjectBuilder()
			  .add("method", "addNode")
			  .add("parameters", params)
			  .build();
		}
		else if(command.getAction().equals("deletenode")) {
	          params = Json.createObjectBuilder()
	          .add("subClusterId", command.getCommandSeries().get(0))
	          .add("ip", command.getCommandSeries().get(1))
	          .add("port", command.getCommandSeries().get(2))
	          .build();
	
	          jobj = Json.createObjectBuilder()
	          .add("method", "deleteNode")
	          .add("parameters", params)
	          .build();
		}
		else if(command.getAction().equals("getnodes")) {
            params = Json.createObjectBuilder()
                    .add("pgid", command.getCommandSeries().get(0))
                    .build();

            jobj = Json.createObjectBuilder()
                    .add("method", "getNodes")
                    .add("parameters", params)
                    .build();
		}
		else if (command.getAction().equals("help")) {
			System.out.println(getHelpText(2));
			return;
		}
		else {
			System.out.println("command not supported");
			return;
		}
    	
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JsonWriter writer = Json.createWriter(baos);
        writer.writeObject(jobj);
        writer.close();
        baos.writeTo(outputStream);

        outputStream.write("\n".getBytes());
        outputStream.flush();

        JsonObject res = parseRequest(input);
        if (res != null) {
            System.out.println();
        	System.out.println("Response received at " + timeStamp + " ---- " + res.toString());
            if (res.containsKey("status") && res.containsKey("message")) {
                System.out.println("REPONSE STATUS: " + res.getString("status") + ", " + "message: " + res.getString("message"));
            }
            System.out.println();
         }
    }
    
    public void processCommandRing(String cmd, int dhtType) throws Exception {
        Command command = new Command(cmd);
        if(command.getAction().equals("help"))
        {
            System.out.println(getHelpText(dhtType));
        }
        else if(command.getAction().equals("exit"))
        {
            System.exit(0);
        }
        else
        {
        	sendCommandStr(cmd, input, output);
        }
    }
    
    public void processCommandRing(String cmd) throws Exception {
    	processCommandRing(cmd, 1);
    }
    
    public void processCommandElastic(String cmd) throws Exception {
    	processCommandRing(cmd, 3);
    }
    
    public void processCommand(int typeDHT, String cmd) throws Exception {
    	switch(typeDHT) {
	    	case 1:
	    		processCommandRing(cmd);
	    		break;
	    	case 2:
	    		processCommandRush(cmd);
	    		break;
	    	case 3:
	    		processCommandElastic(cmd);
	    		break;
    	}
    }
    
    public static String getHelpText(int dhtType) {
    	String tip = "";
    	switch(dhtType) {
	    	case 1:
	    		tip = "\nadd <IP> <Port>\nadd <IP> <Port> <hash>\nremove <hash>\nloadbalance <delta> <hash>\ninfo/help/exit/read file\n";
	    		break;
	    	case 2:
	    		tip = "\naddnode <subClusterId> <IP> <Port> <weight> | example: addnode S0 localhost 689 0.5\ndeletenode <subClusterId> <IP> <Port> | example: deletenode S0 localhost 689\ngetnodes <pgid> | example: getnodes PG1\nhelp\n";
	    		break;
	    	case 3:
	    		tip = "\nadd <IP> <Port>\nadd <IP> <Port> <start> <end>\nremove <IP> <Port>\nloadbalance <fromIP> <fromPort> <toIP> <toPort> <numOfBuckets>\ninfo/help/exit/read file\n";
	    		break;
    	}
    	
    	return tip;
    }
    
    public static void main(String[] args) throws Exception {
        System.out.println("==== Welcome to Data Node !!! =====");
        
    	String serverAddress = "localhost";
    	int port = 9091; 
    	String dhtName = "Ring";
    	int dhtType = 1;
        
    	DataNode client = new DataNode();
    	boolean connected = client.connectServer(serverAddress, port);
		
		if (connected) {
			System.out.println("Connected to " + dhtName + " Server ");
		}
		else {
			System.out.println("Unable to connect to " + dhtName + " server!");
			return;
		}

		Console console = System.console();
        while(true)
        {
        	String cmd = console.readLine("Input your command:");
            
        	client.processCommand(dhtType, cmd);
        }
    }

}
