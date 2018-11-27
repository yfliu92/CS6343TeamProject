package dht.elastic_DHT_centralized;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.HashSet;

import javax.json.*;

import dht.elastic_DHT_centralized.LookupTable;
import dht.common.response.Response;

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
		return lookupTable.buildTable(data);
	}
	
	public void printTableInfo() {
		int items = lookupTable != null ? lookupTable.getBucketsTable().size() : 0;
		int physicalNodes = lookupTable != null ? lookupTable.getPhysicalNodesMap().size() : 0;
		String epoch = items != 0 ? String.valueOf(lookupTable.getEpoch()): "";
		System.out.println("A total of " + items + " hash buckets and " + physicalNodes + " physical nodes found in the table, epoch " + epoch);
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
//        System.out.println("==== Welcome to Data Node !!! =====");
//        
//    	String serverAddress = "localhost";
//    	int port = 9093; 
//    	String dhtName = "Elastic DHT";
//    	int dhtType = 3;
//    	
//    	DataNode dataNode = new DataNode();
//        
//    	DataNodeClient client = new DataNodeClient(dataNode);
//    	boolean connected = client.connectServer(serverAddress, port);
//		
//		if (connected) {
//			System.out.println("Connected to " + dhtName + " Server ");
//			
//			client.sendCommandStr_JsonRes(new Command("dht pull"), client.input, client.output);
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
    	
    	
        System.out.println("==== Welcome to Elastic DHT Data Node !!! =====");
        
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
        System.out.println("Elastic DHT Data Node server running at " + dataNode.IP + ":" + String.valueOf(port));

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
                    this.input.close();
                    this.output.close();
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
            	System.out.println("Disconnected by " + s.getPort() + " ---- " + new Date().toString()); 
//            	System.out.println("Connection reset at " + s.getPort() + " ---- " + new Date().toString());
//                e.printStackTrace(); 
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


