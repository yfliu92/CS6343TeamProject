package dht.rush;

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

import dht.common.response.Response;
import dht.rush.clusters.ClusterStructureMap;
import dht.server.Command;

public class client {
	private ClusterStructureMap clusterStructureMap;
	
	public void printTable() {
		if (clusterStructureMap == null) {
			System.out.println("Local DHT table is not initialized");
			return;
		}
		clusterStructureMap.print();
	}
	
	public boolean buildTable(JsonObject data) {
		clusterStructureMap = new ClusterStructureMap();
		return clusterStructureMap.buildTable(data);
	}
	
	public void printTableInfo() {
		int items = clusterStructureMap != null ? clusterStructureMap.getChildrenList().size() : 0;
		String epoch = items != 0 ? String.valueOf(clusterStructureMap.getEpoch()): "";
		System.out.println("A total of " + items + " hash buckets found in the table, epoch " + epoch);
	}
	
	public boolean isTableLatest(String epoch) {
		String localEpoch = clusterStructureMap != null ? String.valueOf(clusterStructureMap.getEpoch()): "";
		if (localEpoch.equals(epoch)) {
			return true;
		}
		else {
			return false;
		}
	}
	
    public ClusterStructureMap getLookupTable() {
        return clusterStructureMap;
    }
    
    public void setLookupTable(ClusterStructureMap map) {
    	this.clusterStructureMap = map;
    }
	
	public String getDHTEpoch() {
		String epoch = this.clusterStructureMap != null ? String.valueOf(this.clusterStructureMap.getEpoch()) : "";
		return epoch;
	}
	
    public static void main(String[] args) throws Exception {
        System.out.println("==== Welcome to Client !!! =====");
        
        String serverAddress = "localhost";
    	int port = 0; 
    	
    	int dhtType = 2;
    	
    	if (args.length >= 2) {
    		serverAddress = args[0];
    		port = Integer.valueOf(args[1]);
    	}
    	if (args.length == 3) {
    		dhtType = Integer.valueOf(args[2]);
    	}
    	String dhtName = dhtType == 1 ? "DHT Ring" : dhtType == 2 ? "DHT Ceph" : dhtType == 3 ? "Elastic DHT" : "";
    	dhtName += " Data Node";

    	RWClient client = new RWClient();
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

class RWClient {
    PrintWriter output;
    BufferedReader input;
    InputStream inputStream;
    OutputStream outputStream;
	SocketAddress socketAddress;
	Socket socket;
	
    public boolean connectServer(String serverAddress, int port) {
    	int timeout = 2000;
		try {
			socketAddress = new InetSocketAddress(serverAddress, port);
			socket = new Socket();
			socket.connect(socketAddress, timeout);
			inputStream = socket.getInputStream();
			outputStream = socket.getOutputStream();
			output = new PrintWriter(outputStream, true);
			input = new BufferedReader(new InputStreamReader(inputStream));

	        System.out.println("Connected to server " + serverAddress + ":" + port + ", with local port " + socket.getLocalPort());
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
    
    public void sendCommandStr(Command command, BufferedReader input, PrintWriter output) throws Exception {
    	String[] jsonCommands = {"read", "write", "data", "dht", "find", "info", "writebatch", "updatebatch"};
    	for(String jsonCommand: jsonCommands) {
    		if (command.getAction().equals(jsonCommand)) {
    			sendCommandStr_JsonRes(command, input, output);
    			return;
    		}
    	}

    	System.out.println("Sending command" + " ---- " + new Date().toString());
            output.println(command);
            output.flush();
        String response = input.readLine();
        System.out.println("Response received: " + response + " ---- " + new Date().toString());
    }
    
    public void sendCommandJson(Command command, BufferedReader input, PrintWriter output) throws Exception {
    	
    	String timeStamp = new Date().toString();
    	System.out.println("Sending command" + " ---- " + timeStamp);
    	
    	output.println(new Response(true, command.getRawCommand(), command.getRawCommand()).serialize());
    	output.flush();
        
        JsonObject res = parseRequest(input);
        if (res != null) {
            System.out.println();
            System.out.println("Response received at " + timeStamp);
            System.out.println(res.toString());
            parseResponse(res, command, input, output);
        	
        	System.out.println();
         }
    }
    
    public void sendCommandStr_JsonRes(Command command, BufferedReader input, PrintWriter output) throws Exception {
    	
    	String timeStamp = new Date().toString();
    	System.out.println("Sending command" + " ---- " + timeStamp);
    	
        output.println(command.getRawCommand());
        output.flush();
        
        JsonObject res = parseRequest(input);
        if (res != null) {
            System.out.println();
            System.out.println("Response received at " + timeStamp);
            parseResponse(res, command, input, output);
        	
        	System.out.println();
         }
    }
    
    public void parseResponse(JsonObject res, Command command, BufferedReader input, PrintWriter output) throws Exception {
    	if (command.getAction().equals("info")) {
    		String message = "Data Epoch number: ";
    		if (res.containsKey("message") ) {
    			message = res.getString("message");
    		}
    		if (res.containsKey("result")) {
    			System.out.println(message + " --- " + res.getString("result"));
    		}
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
			  .add("method", "addnode")
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
	          .add("method", "deletenode")
	          .add("parameters", params)
	          .build();
		}
		else if(command.getAction().equals("getnodes")) {
            params = Json.createObjectBuilder()
                    .add("pgid", command.getCommandSeries().get(0))
                    .build();

            jobj = Json.createObjectBuilder()
                    .add("method", "getnodes")
                    .add("parameters", params)
                    .build();
		}
		else if (command.getAction().equals("help")) {
			System.out.println(getHelpText(2));
			return;
		}
		else if (command.getAction().equals("dht")) {
			
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
        	sendCommandJson(command, input, output);
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
//	    		processCommandRush(cmd);
	    		processCommandRing(cmd, 2);
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
	    		tip = "\nhelp";
	    		tip += "\nfind <hash>    //find the virtual node on the server corresponding to the hash value";
	    		tip += "\ndht head|pull  //fetch server dht table info";
	    		tip += "\ndht info|list  //show local dht table info";
	    		tip += "\ninfo           //show server dht table info";
	    		tip += "\nexit\n";
	    		break;
	    	case 2:
	    		tip = "\nhelp";
	    		tip += "\naddnode <subClusterId> <IP> <Port> <weight>  //example: addnode S0 localhost 689 0.5";
	    		tip += "\ndeletenode <subClusterId> <IP> <Port>  //example: deletenode S0 localhost 689";
	    		tip += "\ngetnodes <pgid> | example: getnodes PG1";
	    		tip += "\ninfo";
	    		tip += "\nexit\n";
	    		break;
	    	case 3:
	    		tip = "\nhelp";
	    		tip += "\nread <randomStr>";
	    		tip += "\nfind <hash>    //find the virtual node on the server corresponding to the hash value";
	    		tip += "\ndht head|pull  //fetch server dht table info";
	    		tip += "\ndht info|list  //show local dht table info";
	    		tip += "\ninfo           //show server dht table info";
	    		tip += "\nexit\n";
	    		break;
    	}
    	
    	return tip;
    }
}
