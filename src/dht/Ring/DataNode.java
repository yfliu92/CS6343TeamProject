package dht.Ring;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;

import javax.json.*;

import dht.Ring.LookupTable;
import dht.Ring.VirtualNode;
import dht.common.Hashing;
import dht.common.response.Response;
import dht.server.Command;

public class DataNode {
	private LookupTable lookupTable;
	HashSet<Integer> hashBucket;
	
	String IP;
	int port;
	
	public DataNode() {
		hashBucket = new HashSet<Integer>();
	}
	
	public DataNode(String IP, int port) {
		hashBucket = new HashSet<Integer>();
		this.IP = IP;
		this.port = port;
	}
	
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
        System.out.println("==== Welcome to DHT Ring Data Node !!! =====");
        
        DataNode dataNode = null;
        if (args.length == 2) {
        	try {
        		dataNode = new DataNode(args[0], Integer.valueOf(args[1]));
        	}
        	catch (Exception e) {
        		System.out.println("input parameters <IP> <port> to start data node");
        		return;
        	}
        }
        else {
        	dataNode = new DataNode();
        }

		int port = dataNode.port;
        ServerSocket ss = new ServerSocket(port); 
        System.out.println("Ring Data Node server running at " + dataNode.IP + ":" + String.valueOf(port));

        while (true)
        { 
            Socket s = null; 
              
            try 
            {
                s = ss.accept(); 
                  
                System.out.println("A new client is connected : " + s); 
                  
            	BufferedReader input = new BufferedReader(new InputStreamReader(s.getInputStream()));
		        PrintWriter output = new PrintWriter(s.getOutputStream(), true);
  
                Thread t = new ClientHandler(s, input, output, dataNode); 

                t.start(); 
                  
            } 
            catch (Exception e){ 
                s.close(); 
                e.printStackTrace(); 
            } 
        }
    }

}

//class DataNodeClient {
//    PrintWriter output;
//    BufferedReader input;
//    InputStream inputStream;
//    OutputStream outputStream;
//	SocketAddress socketAddress;
//	Socket socket;
//	DataNode dataNode;
//	
//	public DataNodeClient(DataNode dataNode) {
//		this.dataNode = dataNode;
//	}
//	
//    public boolean connectServer(String serverAddress, int port) {
//    	int timeout = 2000;
//		try {
//			socketAddress = new InetSocketAddress(serverAddress, port);
//			socket = new Socket();
//			socket.connect(socketAddress, timeout);
//			inputStream = socket.getInputStream();
//			outputStream = socket.getOutputStream();
//			output = new PrintWriter(outputStream, true);
//			input = new BufferedReader(new InputStreamReader(inputStream));
//
//	        System.out.println("Connected to server " + serverAddress + ":" + port + ", with local port " + socket.getLocalPort());
////			socket.close();
//			return true;
// 
//		} catch (SocketTimeoutException exception) {
////			socket.close();
//			System.out.println("SocketTimeoutException " + serverAddress + ":" + port + ". " + exception.getMessage());
//			return false;
//		} catch (IOException exception) {
////			socket.close();
//			System.out.println(
//					"IOException - Unable to connect to " + serverAddress + ":" + port + ". " + exception.getMessage());
//			return false;
//		}
//    }
//    
//    public void sendCommandStr(Command command, BufferedReader input, PrintWriter output) throws Exception {
//    	if (processLocalRequest(command)) {
//    		return;
//    	}
//    	
//    	String[] jsonCommands = {"read", "write", "data", "dht", "find", "info", "writebatch", "updatebatch"};
//    	for(String jsonCommand: jsonCommands) {
//    		if (command.getAction().equals(jsonCommand)) {
//    			sendCommandStr_JsonRes(command, input, output);
//    			return;
//    		}
//    	}
//
//    	System.out.println("Sending command" + " ---- " + new Date().toString());
//            output.println(command);
//            output.flush();
//        String response = input.readLine();
//        System.out.println("Response received: " + response + " ---- " + new Date().toString());
//    }
//    
//    public void sendCommandStr_JsonRes(Command command, BufferedReader input, PrintWriter output) throws Exception {
//    	
//    	String timeStamp = new Date().toString();
//    	System.out.println("Sending command" + " ---- " + timeStamp);
//        output.println(command.getRawCommand());
//        output.flush();
//        
//        JsonObject res = parseRequest(input);
//        if (res != null) {
//            System.out.println();
//            System.out.println("Response received at " + timeStamp);
//            parseResponse(res, command, input, output);
//        	
//        	System.out.println();
//         }
//    }
//    
//    public boolean processLocalRequest(Command command) {
//    	if (command.getAction().equals("dht")) {
//    		if (command.getCommandSeries().size() > 0) {
//    			if (command.getCommandSeries().get(0).equals("info")) {
//    				this.dataNode.printTableInfo();
//    				return true;
//    			}
//    			else if (command.getCommandSeries().get(0).equals("list")) {
//    				this.dataNode.printTable();
//    				return true;
//    			}
//    		}
//    	}
//    	
//    	return false;
//    }
//    
//    public void parseResponse(JsonObject res, Command command, BufferedReader input, PrintWriter output) throws Exception {
//    	if (command.getAction().equals("dht")) {
//    		String action2 = command.getCommandSeries().size() > 0 ? command.getCommandSeries().get(0) : "head";
//    		if (action2.equals("pull")) {
//    			dataNode.buildTable(res.getJsonObject("jsonResult"));
//    			System.out.println("DHT table pulled successfully");
//    			dataNode.printTableInfo();
//    			return;
//    		}
//    		else if (action2.equals("head")) {
//    			String latestEpoch = res.getString("result");
//    			if (dataNode.isTableLatest(latestEpoch)) {
//    				System.out.println("DHT table is already the latest");
//    				dataNode.printTableInfo();
//    			}
//    			else {
//    				System.out.println("DHT table is outdated");
//    				System.out.println("Latest epoch number: " + latestEpoch);
//    				System.out.println("Local epoch number: " + dataNode.getDHTEpoch());
//    			}
//    			return;
//    		}
//    	}
//    	else if (command.getAction().equals("read")) {
//    		if (command.getCommandSeries().size() > 0) {
//    			String dataStr = command.getCommandSeries().get(0);
//    			
//    			int rawhash = Hashing.getHashValFromKeyword(dataStr);
//    			try {
//    				rawhash = Integer.valueOf(dataStr);
//    			}
//    			catch (Exception e) {
//    				
//    			}
//    			
//    			int[] virtualnodeids = this.dataNode.getLookupTable().getTable().getVirtualNodeIds(rawhash);
//    			String virtualnodeids_remote = res.getString("result");
//    			System.out.println("Raw hash of " + dataStr + ": " + rawhash);
//    			System.out.println("Virtual Node Ids from Local DHT: " + Arrays.toString(virtualnodeids));
//    			if (virtualnodeids_remote.indexOf(String.valueOf(virtualnodeids[0])) < 0) {
////    			if (!Arrays.toString(virtualnodeids).equals(virtualnodeids_remote)) {
//    				System.out.println("Local DHT is outdated");
//    				System.out.println("Virtual Node Ids from Remote DHT: " + virtualnodeids_remote);
//    				System.out.println("Starting to update DHT...");
//    				sendCommandStr(new Command("dht pull"), input, output);
//    			}
//    			return;
//    		}
//    	}
//    	else if (command.getAction().equals("find")) {
//    		int hash = command.getCommandSeries().size() > 0 ? Integer.valueOf(command.getCommandSeries().get(0)) : -1;
//    		String localVirtualNode = hash >= 0 ? String.valueOf(this.dataNode.getLookupTable().getTable().find(hash).getHash()) : "";
//    		String virtualNode = res.getJsonObject("jsonResult").get("hash").toString();
//    		String physicalNode = res.getJsonObject("jsonResult").getString("physicalNodeId");
//    		System.out.println("Node Info: " + physicalNode + " (hash: " + virtualNode + ")");
//    		if (!virtualNode.equals(localVirtualNode)) {
//    			System.out.println("Local DHT is outdated.");
//    		}
//    		else {
//    			System.out.println("Local DHT is update to date.");
//    		}
//    		return;
//    	}
//    	else if (command.getAction().equals("info")) {
//    		String epoch = res.getJsonObject("jsonResult").get("epoch").toString();
//    		System.out.println("DHT table info");
//    		System.out.println("Epoch number: " + epoch);
//    		JsonArray tableResult = res.getJsonObject("jsonResult").get("table").asJsonArray();
//    		for(int i = 0; i < tableResult.size(); i++) {
//    			System.out.println(tableResult.get(i));
//    		}
//    		return;
//    	}
//    	
//    	if (res.containsKey("status")) {
//    		if (res.containsKey("message")) {
//    			System.out.println(res.getString("message"));
//    		}
//    		if (res.containsKey("result")) {
//    			System.out.println(res.getString("result"));
//    		}
//    		if (res.containsKey("jsonResult")) {
//    			System.out.println(res.getJsonObject("jsonResult").toString());
//    		}
//    	}
//    	else {
//    		System.out.println(res.toString());
//    	}
//    }
//	
//    public static JsonObject parseRequest(BufferedReader br) throws Exception {
//        String str;
//        JsonObject jsonObject = null;
//
//        while ((str = br.readLine()) != null) {
//            JsonReader jsonReader = Json.createReader(new StringReader(str));
//            jsonObject = jsonReader.readObject();
//            return jsonObject;
//        }
//        return jsonObject;
//    }
//    
//    public void processCommandRush(String cmd) throws Exception {
//    	Command command = new Command(cmd);
//    	
//    	String timeStamp = new Date().toString();
//    	System.out.println("Sending command" + " ---- " + timeStamp);
//    	System.out.println();
//        
//        JsonObject params = null;
//        JsonObject jobj = null;
//		if(command.getAction().equals("addnode")) {
//			  params = Json.createObjectBuilder()
//			  .add("subClusterId", command.getCommandSeries().get(0))
//			  .add("ip", command.getCommandSeries().get(1))
//			  .add("port", command.getCommandSeries().get(2))
//			  .add("weight", command.getCommandSeries().get(3))
//			  .build();
//			
//			  jobj = Json.createObjectBuilder()
//			  .add("method", "addnode")
//			  .add("parameters", params)
//			  .build();
//		}
//		else if(command.getAction().equals("deletenode")) {
//	          params = Json.createObjectBuilder()
//	          .add("subClusterId", command.getCommandSeries().get(0))
//	          .add("ip", command.getCommandSeries().get(1))
//	          .add("port", command.getCommandSeries().get(2))
//	          .build();
//	
//	          jobj = Json.createObjectBuilder()
//	          .add("method", "deletenode")
//	          .add("parameters", params)
//	          .build();
//		}
//		else if(command.getAction().equals("getnodes")) {
//            params = Json.createObjectBuilder()
//                    .add("pgid", command.getCommandSeries().get(0))
//                    .build();
//
//            jobj = Json.createObjectBuilder()
//                    .add("method", "getnodes")
//                    .add("parameters", params)
//                    .build();
//		}
//		else if (command.getAction().equals("help")) {
//			System.out.println(getHelpText(2));
//			return;
//		}
//		else {
//			System.out.println("command not supported");
//			return;
//		}
//    	
//        ByteArrayOutputStream baos = new ByteArrayOutputStream();
//        JsonWriter writer = Json.createWriter(baos);
//        writer.writeObject(jobj);
//        writer.close();
//        baos.writeTo(outputStream);
//
//        outputStream.write("\n".getBytes());
//        outputStream.flush();
//
//        JsonObject res = parseRequest(input);
//        if (res != null) {
//            System.out.println();
//        	System.out.println("Response received at " + timeStamp + " ---- " + res.toString());
//            if (res.containsKey("status") && res.containsKey("message")) {
//                System.out.println("REPONSE STATUS: " + res.getString("status") + ", " + "message: " + res.getString("message"));
//            }
//            System.out.println();
//         }
//    }
//    
//    public void processCommandRing(String cmd, int dhtType) throws Exception {
//        Command command = new Command(cmd);
//        if(command.getAction().equals("help"))
//        {
//            System.out.println(getHelpText(dhtType));
//        }
//        else if(command.getAction().equals("exit"))
//        {
//            System.exit(0);
//        }
//        else
//        {
//        	sendCommandStr(command, input, output);
//        }
//    }
//    
//    public void processCommandRing(String cmd) throws Exception {
//    	processCommandRing(cmd, 1);
//    }
//    
//    public void processCommandElastic(String cmd) throws Exception {
//    	processCommandRing(cmd, 3);
//    }
//    
//    public void processCommand(int typeDHT, String cmd) throws Exception {
//    	switch(typeDHT) {
//	    	case 1:
//	    		processCommandRing(cmd);
//	    		break;
//	    	case 2:
//	    		processCommandRush(cmd);
//	    		break;
//	    	case 3:
//	    		processCommandElastic(cmd);
//	    		break;
//    	}
//    }
//    
//    public static String getHelpText(int dhtType) {
//    	String tip = "";
//    	switch(dhtType) {
//	    	case 1:
//	    		tip = "\nhelp";
//	    		tip += "\nread <randomStr>";
//	    		tip += "\nfind <hash>    //find the virtual node on the server corresponding to the hash value";
//	    		tip += "\ndht head|pull  //fetch server dht table info";
//	    		tip += "\ndht info|list  //show local dht table info";
//	    		tip += "\ninfo           //show server dht table info";
//	    		tip += "\nexit\n";
//	    		break;
//	    	case 2:
//	    		tip = "\nhelp";
//	    		tip += "\naddnode <subClusterId> <IP> <Port> <weight>  //example: addnode S0 localhost 689 0.5";
//	    		tip += "\ndeletenode <subClusterId> <IP> <Port>  //example: deletenode S0 localhost 689";
//	    		tip += "\ngetnodes <pgid> | example: getnodes PG1";
//	    		tip += "\ninfo";
//	    		tip += "\nexit\n";
//	    		break;
//	    	case 3:
//	    		tip = "\nhelp";
//	    		tip += "\nadd <IP> <Port>\nadd <IP> <Port> <start> <end>\nremove <IP> <Port>";
//	    		tip += "\nloadbalance <fromIP> <fromPort> <toIP> <toPort> <numOfBuckets>";
//	    		tip += "\ninfo";
//	    		tip += "\nexit\n";
//	    		break;
//    	}
//    	
//    	return tip;
//    }
//}

class ClientHandler extends Thread  
{
    final BufferedReader input;
    final PrintWriter output;
    final Socket s; 
    final DataNode dataNode;
  
    public ClientHandler(Socket s, BufferedReader input, PrintWriter output, DataNode dataNode) {
    	this.s = s; 
    	this.input = input;
        this.output = output;
        this.dataNode = dataNode;
    }
  
    @Override
    public void run()  
    { 
        String msg; 
        while (true)  
        { 
            try {
            	msg = input.readLine(); 
                if (msg == null) {
            		System.out.println("Connection end " + " ---- " + new Date().toString());
            		break;
                }
                  
                if (msg.equals("Exit")) 
                {  
                    System.out.println("Client " + this.s + " sends exit..."); 
                    System.out.println("Closing this connection."); 
                    this.s.close(); 
                    System.out.println("Connection closed by " + s.getPort()); 
                    break; 
                }
                else { // msg != null
                	System.out.println("Request received from " + s.getPort() + ": " + msg + " ---- " + new Date().toString());
                	System.out.println();
                	
                    JsonReader jsonReader = Json.createReader(new StringReader(msg));
                    JsonObject requestObject = jsonReader.readObject();
                	
                	String response = getResponse(requestObject);

                	output.println(response);
                	output.flush();
                	
                    System.out.println("Response sent to " + s.getPort() + ": " + response + " ---- " + new Date().toString());
                    System.out.println();
            	}
          
            } catch (Exception e) { 
            	System.out.println("Connection reset at " + s.getPort() + " ---- " + new Date().toString());
                e.printStackTrace(); 
        		break;
            } 
        } 
          
        try
        { 
        	this.output.close();
        	this.input.close();
              
        }catch(IOException e){ 
            e.printStackTrace(); 
        } 
    } 
    
    public static JsonObject parseRequest(BufferedReader br) throws Exception {
        String str;
        JsonObject jsonObject = null;

        while ((str = br.readLine()) != null) {
        	System.out.println(str);
            JsonReader jsonReader = Json.createReader(new StringReader(str));
            jsonObject = jsonReader.readObject();
            return jsonObject;
        }
        return jsonObject;
    }
    
    public String getResponse(JsonObject jsonCommand) {  
    	try {
    		String commandStr = jsonCommand.containsKey("message") ? jsonCommand.getString("message") : "";
			if (commandStr.equals("dht push")) {
				if (dataNode.buildTable(jsonCommand.getJsonObject("jsonResult"))) {
					return new Response(true, "DHT updated successfully at " + dataNode.IP + ":" + dataNode.port + ", latest epoch number: " + dataNode.getDHTEpoch()).serialize();
				}
				else {
					return new Response(false, "Failed updating DHT at " + dataNode.IP + ":" + dataNode.port + ", current epoch number: " + dataNode.getDHTEpoch()).serialize();
				}
			}
			else if (commandStr.equals("dht head")) {
				return new Response(true, dataNode.getDHTEpoch(), "DHT Epoch from Data Node " + dataNode.IP + ":" + dataNode.port).serialize();
			}
			else if (commandStr.equals("info")) {
				if (dataNode.getLookupTable() != null) {
					return new Response(true, dataNode.getLookupTable().toJSON(), "DHT Table from Data Node " + dataNode.IP + ":" + dataNode.port).serialize();
				}
				else {
					return new Response(false, "DHT table not initialized").serialize();
				}
			}
			else {
				return new Response(false, "Command not supported").serialize();
			}
    	}
    	catch (Exception e) {
    		return new Response(false, e.toString()).serialize();
    	}

    }
    
//	public String getResponse(String commandStr) {
//		Command command = new Command(commandStr);
//		try {
//
//			if (command.getAction().equals("dht")) {
//				String operation = command.getCommandSeries().size() > 0 ? command.getCommandSeries().get(0) : "head";
//				if (operation.equals("head")) {
//					return new Response(true, String.valueOf(this.dataNode.getLookupTable().getEpoch()), "Current epoch number:").serialize();
//				}
//				else if (operation.equals("pull")) {
////						return super.getLookupTable().serialize();
//					return new Response(true, this.dataNode.getLookupTable().toJSON(), "Ring DHT table").serialize();
//				}
//				else if (operation.equals("push")) {
////						return super.getLookupTable().serialize();
//					
//					return new Response(true, "ready", "Ring DHT table").serialize();
//				}
//				else if (operation.equals("print")) {
//					this.dataNode.getLookupTable().print();
//					return new Response(true, "DHT printed on server").serialize();
//				}
//				else {
//					return new Response(false, "Command not supported").serialize();
//				}
//			
//			}
//			else if (command.getAction().equals("read")) {
//				String dataStr = command.getCommandSeries().get(0);
//				int rawhash = Hashing.getHashValFromKeyword(dataStr);
//				try {
//					rawhash = Integer.valueOf(dataStr);
//				}
//				catch (Exception e) {
//					
//				}
//	
//				int[] virtualnodeids = this.dataNode.getLookupTable().getTable().getVirtualNodeIds(rawhash);
//				return new Response(true, Arrays.toString(virtualnodeids), "Virtual Node IDs from Data Node").serialize();
//			}
//			else if (command.getAction().equals("write")) {
//				return "Command not supported";
//			}
////				else if (command.getAction().equals("read")) {
////					String dataStr = command.getCommandSeries().get(0);
//////					return dataStore.readRes(dataStr);
////					
////	    			int rawhash = Hashing.getHashValFromKeyword(dataStr);
////	    			try {
////	    				rawhash = Integer.valueOf(dataStr);
////	    			}
////	    			catch (Exception e) {
////	    				
////	    			}
////
////	    			int[] virtualnodeids = super.getLookupTable().getTable().getVirtualNodeIds(rawhash);
////	    			return new Response(true, Arrays.toString(virtualnodeids), "Virtual Node IDs from server").serialize();
////					
////				}
////				else if (command.getAction().equals("write")) {
////					String dataStr = command.getCommandSeries().get(0);
////					int rawhash = Hashing.getHashValFromKeyword(dataStr);
////					int[] virtualnodeids = super.getLookupTable().getTable().getVirtualNodeIds(rawhash);
////					
////					return dataStore.writeRes(dataStr, rawhash, virtualnodeids);
////				}
//			else if (command.getAction().equals("loadbalance")) {
//				return "Command not supported";
//			}
//			else if (command.getAction().equals("info")) {
//				return new Response(true, this.dataNode.getLookupTable().toJSON(), "DHT Table from Server").serialize();
//			}
//			else {
//				return "Command not supported";
//			}
//		}
//		catch (Exception ee) {
//			return "Illegal command " + ee.toString();
//		}
//
//	}
}
