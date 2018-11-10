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
import java.util.Arrays;
import java.util.Date;

import javax.json.*;

import dht.Ring.LookupTable;
import dht.Ring.VirtualNode;
import dht.common.Hashing;
import dht.server.Command;

public class DataNode {
	private LookupTable lookupTable;
	
	public void printTable() {
		if (lookupTable == null) {
			System.out.println("Local DHT table is not initialized");
			return;
		}
		lookupTable.print();
	}
	
	public boolean buildTable(JsonObject data) {
		lookupTable = new LookupTable();
		if (data.containsKey("epoch")) {
			this.lookupTable.setEpoch(Long.valueOf(data.get("epoch").toString()));
		}
		if (data.containsKey("table")) {
			JsonArray jsonArray = data.get("table").asJsonArray();
			for(int i = 0; i < jsonArray.size(); i++) {
				lookupTable.getTable().add(new VirtualNode(jsonArray.getJsonObject(i)));
			}
		}
		
		return true;
	}
	
	public void printTableInfo() {
		int items = lookupTable != null ? lookupTable.getTable().size() : 0;
		String epoch = items != 0 ? String.valueOf(lookupTable.getEpoch()): "";
		System.out.println("A total of " + items + " records found in the table, epoch " + epoch);
	}
	
	public boolean isTableLatest(String epoch) {
		String localEpoch = lookupTable != null ? String.valueOf(lookupTable.getEpoch()): "";
		if (localEpoch.equals(epoch)) {
			return true;
		}
		else {
			return false;
		}
	}
	
    public LookupTable getLookupTable() {
        return lookupTable;
    }
	
	public String getDHTEpoch() {
		String epoch = this.lookupTable != null ? String.valueOf(this.lookupTable.getEpoch()) : "";
		return epoch;
	}
    
    public static void main(String[] args) throws Exception {
        System.out.println("==== Welcome to Data Node !!! =====");
        
    	String serverAddress = "localhost";
    	int port = 9091; 
    	String dhtName = "Ring";
    	int dhtType = 1;
    	
    	DataNode dataNode = new DataNode();
        
    	DataNodeClient client = new DataNodeClient(dataNode);
    	boolean connected = client.connectServer(serverAddress, port);
		
		if (connected) {
			System.out.println("Connected to " + dhtName + " Server ");
			
			client.sendCommandStr_JsonRes(new Command("dht pull"), client.input, client.output);
			
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

class DataNodeClient {
    PrintWriter output;
    BufferedReader input;
    InputStream inputStream;
    OutputStream outputStream;
	SocketAddress socketAddress;
	Socket socket;
	DataNode dataNode;
	
	public DataNodeClient(DataNode dataNode) {
		this.dataNode = dataNode;
	}
	
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
    	if (processLocalRequest(command)) {
    		return;
    	}
    	
    	String[] jsonCommands = {"read", "write", "data", "dht", "info", "writebatch", "updatebatch"};
    	for(String jsonCommand: jsonCommands) {
    		if (command.getAction().equals(jsonCommand)) {
    			sendCommandStr_JsonRes(command, input, output);
    			return;
    		}
    	}

    	System.out.println("Sending command" + " ---- " + new Date().toString());
            output.println(command);
        String response = input.readLine();
        System.out.println("Response received: " + response + " ---- " + new Date().toString());
    }
    
    public void sendCommandStr_JsonRes(Command command, BufferedReader input, PrintWriter output) throws Exception {
    	
    	String timeStamp = new Date().toString();
    	System.out.println("Sending command" + " ---- " + timeStamp);
        output.println(command.getRawCommand());
        
        JsonObject res = parseRequest(input);
        if (res != null) {
            System.out.println();
            System.out.println("Response received at " + timeStamp);
            parseResponse(res, command);
        	
        	System.out.println();
         }
    }
    
    public boolean processLocalRequest(Command command) {
    	if (command.getAction().equals("dht")) {
    		if (command.getCommandSeries().size() > 0) {
    			if (command.getCommandSeries().get(0).equals("info")) {
    				this.dataNode.printTableInfo();
    				return true;
    			}
    			else if (command.getCommandSeries().get(0).equals("list")) {
    				this.dataNode.printTable();
    				return true;
    			}
    		}
    	}
    	
    	return false;
    }
    
    public void parseResponse(JsonObject res, Command command) {
    	if (command.getAction().equals("dht")) {
    		String action2 = command.getCommandSeries().size() > 0 ? command.getCommandSeries().get(0) : "head";
    		if (action2.equals("pull")) {
    			dataNode.buildTable(res.getJsonObject("jsonResult"));
    			System.out.println("DHT table pulled successfully");
    			dataNode.printTableInfo();
    			return;
    		}
    		else if (action2.equals("head")) {
    			String latestEpoch = res.getString("result");
    			if (dataNode.isTableLatest(latestEpoch)) {
    				System.out.println("DHT table is already the latest");
    				dataNode.printTableInfo();
    			}
    			else {
    				System.out.println("DHT table is outdated");
    				System.out.println("Latest epoch number: " + latestEpoch);
    				System.out.println("Local epoch number: " + dataNode.getDHTEpoch());
    			}
    			return;
    		}
    	}
    	else if (command.getAction().equals("read")) {
    		if (command.getCommandSeries().size() > 0) {
    			String dataStr = command.getCommandSeries().get(0);
    			
    			int rawhash = Hashing.getHashValFromKeyword(dataStr);
    			try {
    				rawhash = Integer.valueOf(dataStr);
    			}
    			catch (Exception e) {
    				
    			}
    					
    			int[] virtualnodeids = this.dataNode.getLookupTable().getTable().getVirtualNodeIds(rawhash);
    			System.out.println("Raw hash of " + dataStr + ": " + rawhash);
    			System.out.println("Virtual Node Ids from Local DHT: " + Arrays.toString(virtualnodeids));
    		}
    	}
    	
    	if (res.containsKey("status")) {
    		if (res.containsKey("message")) {
    			System.out.println(res.getString("message"));
    		}
    		if (res.containsKey("result")) {
    			System.out.println(res.getString("result"));
    		}
    		if (res.containsKey("jsonResult")) {
    			System.out.println(res.getJsonObject("jsonResult").toString());
    		}
    	}
    	else {
    		System.out.println(res.toString());
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
        	sendCommandStr(command, input, output);
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
	    		tip = "\nhelp";
	    		tip += "\nadd <IP> <Port>\nadd <IP> <Port> <hash>\nremove <hash>";
	    		tip += "\nloadbalance <delta> <hash>";
	    		tip += "\ndht head|pull  //fetch server dht table info";
	    		tip += "\ndht info|list  //show local dht table info";
	    		tip += "\ninfo";
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
	    		tip += "\nadd <IP> <Port>\nadd <IP> <Port> <start> <end>\nremove <IP> <Port>";
	    		tip += "\nloadbalance <fromIP> <fromPort> <toIP> <toPort> <numOfBuckets>";
	    		tip += "\ninfo";
	    		tip += "\nexit\n";
	    		break;
    	}
    	
    	return tip;
    }
}
