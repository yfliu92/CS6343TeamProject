package dht.rush;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.Console;
import java.io.File;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import javax.json.*;

import dht.common.response.Response;
import dht.rush.clusters.ClusterStructureMap;
import dht.rush.utils.ConfigurationUtil;
import dht.server.Command;

public class DataNode {
    private ClusterStructureMap clusterStructureMap;
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
//        System.out.println("==== Welcome to Data Node !!! =====");
//        
//    	String serverAddress = "localhost";
//    	int port = 8100; 
//    	String dhtName = "Ceph DHT";
//    	int dhtType = 2;
//    	
//    	DataNode dataNode = new DataNode();
//       
//        String rootPath = System.getProperty("user.dir");
////        String xmlPath = rootPath + File.separator + "src" + File.separator + "dht" + File.separator + "rush" + File.separator + "ceph_config.xml";
//
//        String xmlPath = rootPath + File.separator + "dht" + File.separator + "rush" + File.separator + "ceph_config.xml";
//        dataNode.setLookupTable(ConfigurationUtil.parseConfig(xmlPath));
//
//        if (dataNode.getLookupTable() == null) {
//            System.out.println("Central Server initialization failed");
//            System.exit(-1);
//        }
//
//    	DataNodeClient client = new DataNodeClient(dataNode);
//    	boolean connected = client.connectServer(serverAddress, port);
//    	
//    	
//    	
//    	
//		
//		if (connected) {
//			System.out.println("Connected to " + dhtName + " Server ");
//			
////			client.sendCommandStr_JsonRes(new Command("dht pull"), client.input, client.output);
//			
//		}
//		else {
//			System.out.println("Unable to connect to " + dhtName + " server!");
//			return;
//		}
//
//		Console console = System.console();
//        while(true)
//        {
//        	String cmd = console.readLine("Input your command:");
//            
//        	client.processCommand(dhtType, cmd);
//        }
    	
        System.out.println("==== Welcome to Rush DHT Data Node !!! =====");
        
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
        System.out.println("Rush DHT Data Node server running at " + dataNode.IP + ":" + String.valueOf(port));

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
////    	String[] jsonCommands = {"read", "write", "data", "dht", "find", "info", "writebatch", "updatebatch"};
////    	for(String jsonCommand: jsonCommands) {
////    		if (command.getAction().equals(jsonCommand)) {
////    			sendCommandStr_JsonRes(command, input, output);
////    			return;
////    		}
////    	}
//    	
//    	sendCommandStr_JsonRes(command, input, output);
//
////    	System.out.println("Sending command" + " ---- " + new Date().toString());
////            output.println(command);
////        String response = input.readLine();
////        System.out.println("Response received: " + response + " ---- " + new Date().toString());
//    }
//    
//    public void sendCommandStr_JsonRes(Command command, BufferedReader input, PrintWriter output) throws Exception {
//    	
//    	String timeStamp = new Date().toString();
//    	System.out.println("Sending command" + " ---- " + timeStamp);
//        output.println(command.getRawCommand());
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
//    			String latestEpoch = String.valueOf(res.getJsonObject("jsonResult").getInt("epoch"));
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
//    			String nodeinfo = this.dataNode.getLookupTable().read(dataStr).toString();
//    			String nodeinfo_remote = res.getString("destination");
//    			System.out.println("Node Info from Local DHT: " + nodeinfo);
//    			if (!nodeinfo.equals(nodeinfo_remote)) {
//    				System.out.println("Local DHT is outdated");
//    				System.out.println("Node Info from Remote DHT: " + nodeinfo_remote);
//    				System.out.println("Starting to update DHT...");
//    				sendCommandStr(new Command("dht pull"), input, output);
//    			}
//    			return;
//    		}
//    	}
////    	else if (command.getAction().equals("find")) {
////    		String pgid = command.getCommandSeries().size() > 0 ? command.getCommandSeries().get(0) : "";
////    		String localPhysicalNode = !pgid.equals("") ? this.dataNode.getLookupTable().getNodesInfoByPGID(pgid) : "";
////    		String physicalNode = res.getString("result");
////    		System.out.println("Cluster Info: " + physicalNode + " (pgid: " + pgid + ")");
////    		if (!physicalNode.equals(localPhysicalNode)) {
////    			System.out.println("Local DHT is outdated.");
////    			System.out.println("Server cluster info: " + physicalNode);
////    		}
////    		else {
////    			System.out.println("Local DHT is update to date.");
////    		}
////    		return;
////    	}
////    	else if (command.getAction().equals("info")) {
////    		JsonObject data = res.getJsonObject("jsonResult");
////    		String epoch = data.get("epoch").toString();
////    		System.out.println("DHT table info");
////    		System.out.println("Epoch number: " + epoch);
////			System.out.println("Hash buckets: ");
////			JsonObject table = data.getJsonObject("bucketsTable");
////			for(Entry<String, JsonValue> node: table.entrySet()) {
////				System.out.print(node.getKey() + ": ");
////				JsonObject hashPairMap = node.getValue().asJsonObject();
////				for(Entry<String, JsonValue> pair: hashPairMap.entrySet()) {
////					System.out.print("<" + pair.getKey() + ", " + pair.getValue() + "> ");
////				}
////				System.out.print("\n");
////			}
////			System.out.println("Physical nodes: ");
////			JsonObject nodes = data.getJsonObject("physicalNodesMap");
////			for(Entry<String, JsonValue> node: nodes.entrySet()) {
////				JsonObject nodeJson = node.getValue().asJsonObject();
////				System.out.println(nodeJson);
////			}
////    		return;
////    	}
//    	
//    	System.out.println(res.toString());
////    	if (res.containsKey("status")) {
////    		if (res.containsKey("message")) {
////    			System.out.println(res.getString("message"));
////    		}
////    		if (res.containsKey("result")) {
////    			System.out.println(res.getString("result"));
////    		}
////    		if (res.containsKey("jsonResult")) {
////    			System.out.println(res.getJsonObject("jsonResult").toString());
////    		}
////    	}
////    	else {
////    		System.out.println(res.toString());
////    	}
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
//    	if (processLocalRequest(command)) {
//    		return;
//    	}
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
//			  .add("method", "addNode")
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
//	          .add("method", "deleteNode")
//	          .add("parameters", params)
//	          .build();
//		}
//		else if(command.getAction().equals("getnodes")) {
//            params = Json.createObjectBuilder()
//                    .add("pgid", command.getCommandSeries().get(0))
//                    .build();
//
//            jobj = Json.createObjectBuilder()
//                    .add("method", "getNodes")
//                    .add("parameters", params)
//                    .build();
//		}
////		else if(command.getAction().equals("loadbalancing")) {
////			params = Json.createObjectBuilder()
////					.add("subClusterId", command.getCommandSeries().get(0))
////					.build();
////			jobj = Json.createObjectBuilder()
////                    .add("method", "loadbalancing")
////                    .add("parameters", params)
////                    .build();
////		}
//		else if (command.getAction().equals("write")) {
//			params = Json.createObjectBuilder()
//					.add("fileName", command.getCommandSeries().get(0))
//					.build();
//			jobj = Json.createObjectBuilder()
//                    .add("method", "write")
//                    .add("parameters", params)
//                    .build();
//		}
//		else if (command.getAction().equals("read")) {
//			params = Json.createObjectBuilder()
//					.add("fileName", command.getCommandSeries().get(0))
//					.build();
//			jobj = Json.createObjectBuilder()
//                    .add("method", "read")
//                    .add("parameters", params)
//                    .build();
//		}
//		else if (command.getAction().equals("getmap")) {
//			params = Json.createObjectBuilder()
//					.build();
//			jobj = Json.createObjectBuilder()
//                    .add("method", "getmap")
//                    .add("parameters", params)
//                    .build();
//		}
////		else if (command.getAction().equals("changeweight")) {
////			params = Json.createObjectBuilder()
////					.add("subClusterId", command.getCommandSeries().get(0))
////					.add("ip", command.getCommandSeries().get(1))
////					.add("port", command.getCommandSeries().get(2))
////					.add("weight", command.getCommandSeries().get(3))
////					.build();
////			jobj = Json.createObjectBuilder()
////                    .add("method", "changeweight")
////                    .add("parameters", params)
////                    .build();
////		}
//		else if (command.getAction().equals("dht")) {
//			params = Json.createObjectBuilder()
//					.add("fetchtype", command.getCommandSeries().get(0))
//					.build();
//			jobj = Json.createObjectBuilder()
//                    .add("method", "dht")
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
//    	String timeStamp = new Date().toString();
//    	System.out.println("Sending command" + " ---- " + timeStamp);
//    	System.out.println();
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
//            
//            parseResponse(res, command, input, output);
//            
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
//	    		tip += "\nfind <hash>    //find the virtual node on the server corresponding to the hash value";
//	    		tip += "\ndht head|pull|print  //fetch server dht table info";
//	    		tip += "\ndht info|list  //show local dht table info";
//	    		tip += "\ninfo           //show server dht table info";
//	    		tip += "\nexit\n";
//	    		break;
//	    	case 2:
//	    		tip = "\nhelp";
//	    		tip += "\nread <randomStr>";
//	    		tip += "\nwrite <randomStr>";
//	    		tip += "\ngetnodes <pgid> | example: getnodes PG1";
//	    		tip += "\ndht head|pull|print  //fetch server dht table info";
//	    		tip += "\ndht info|list  //show local dht table info";
//	    		tip += "\ngetmap";
//	    		tip += "\nexit\n";
//	    		break;
//	    	case 3:
//	    		tip = "\nhelp";
//	    		tip += "\nfind <hash>    //find the virtual node on the server corresponding to the hash value";
//	    		tip += "\ndht head|pull|print  //fetch server dht table info";
//	    		tip += "\ndht info|list  //show local dht table info";
//	    		tip += "\ninfo           //show server dht table info";
//	    		tip += "\nexit\n";
//	    		break;
//    	}
//    	
//    	return tip;
//    }
//}
