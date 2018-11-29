package dht.elastic_DHT_centralized;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

import javax.json.*;

import dht.elastic_DHT_centralized.LookupTable;
import dht.server.Command;
import dht.common.Hashing;
import dht.common.response.Response;

public class DataNode {
	private LookupTable lookupTable;
	HashSet<Integer> hashBucket;
	
	String IP;
	int port;
	int epoch = 0;
	
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
		hashBucket = new HashSet<Integer>();
		boolean result = lookupTable.buildTable(data);
		buildHashBucket();
		return result;
	}
	
	public void buildHashBucket() {
		if (this.lookupTable.getBucketsTable() != null && this.lookupTable.getBucketsTable().size() > 0) {
			for(HashMap.Entry<Integer, HashMap<String, String>> bucket: lookupTable.getBucketsTable().entrySet()) {
				int hash = Integer.valueOf(bucket.getKey());
				for(HashMap.Entry<String, String> pair: bucket.getValue().entrySet()) {
					String[] physicalNodeInfo = pair.getValue().replaceAll("\"", "").split("-");
					String IP = physicalNodeInfo[0];
					int port = Integer.valueOf(physicalNodeInfo[1]);
					if (port == this.port && IP.equals(this.IP)) 
					{
						hashBucket.add(hash);
					}
				}
			}
		}
	}
	
	public String findNodeInfo(int rawhash) {
		String info = "";
		if (this.lookupTable.getBucketsTable() != null && this.lookupTable.getBucketsTable().size() > 0) {
			HashMap<String, String> physicalNodes = this.lookupTable.getBucketsTable().get(rawhash);
			for(HashMap.Entry<String, String> pair: physicalNodes.entrySet()) {
				info += pair.getValue();
				info += "; ";
			}
		}
		return info;
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
	
	public int getDataEpoch() {
		return this.epoch;
	}
	
	public String getHashBucket() {
		return Arrays.toString(this.hashBucket.toArray());
	}
    
    public static void main(String[] args) throws Exception {
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
					String requestStr = msg.length() > 200 ? msg.substring(0, 200) + "...": msg; 

                	System.out.println("Request received from " + s.getPort() + ": " + requestStr + " ---- " + new Date().toString());
                	System.out.println();
                	
                    JsonReader jsonReader = Json.createReader(new StringReader(msg));
                    JsonObject requestObject = jsonReader.readObject();
                	
                	String response = getResponse(requestObject);

                	output.println(response);
                	output.flush();

					String responseStr = response.length() > 200 ? response.substring(0, 200) + "...": response; 

                    System.out.println("Response sent to " + s.getPort() + ": " + responseStr + " ---- " + new Date().toString());
                    System.out.println();
            	}
          
            } catch (Exception e) { 
            	System.out.println("Disconnected with " + s.getPort() + " ---- " + new Date().toString()); 
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
    		Command command = new Command(commandStr);
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
			else if (commandStr.equals("dht pull")) {
				if (dataNode.getLookupTable() != null) {
					return new Response(true, dataNode.getLookupTable().toJSON(), "DHT Table from Data Node " + dataNode.IP + ":" + dataNode.port).serialize();
				}
				else {
					return new Response(false, "DHT table not initialized").serialize();
				}
			}
			else if (commandStr.equals("info epoch")) {
				return new Response(true, String.valueOf(dataNode.getDataEpoch()), "Data Epoch from Data Node " + dataNode.IP + ":" + dataNode.port).serialize();
			}
			else if (commandStr.equals("info bucket")) {
				return new Response(true, dataNode.getHashBucket(), "Hash Bucket from Data Node " + dataNode.IP + ":" + dataNode.port).serialize();
			}
			else if (command.getAction().equals("read")) {
				String dataStr = command.getCommandSeries().get(0);
				int rawhash = Hashing.getHashValFromKeyword(dataStr);
				try {
					rawhash = Integer.valueOf(dataStr);
				}
				catch (Exception e) {
					
				}
	
				boolean isFound = dataNode.hashBucket.contains(rawhash) ? true : false;
				String message = dataStr + " (hash value: " + rawhash + ") read from this Data Node " + dataNode.IP + ":" + dataNode.port;
				if (!isFound) {
					message = dataStr + " (hash value: " + rawhash + ") not found in this Data Node " + dataNode.IP + ":" + dataNode.port + ".";
					message += " It can be found in Data Node " + dataNode.findNodeInfo(rawhash);
				}
				return new Response(true, message).serialize();
			}
			else if (command.getAction().equals("write")) {
				String dataStr = command.getCommandSeries().get(0);
				int rawhash = Hashing.getHashValFromKeyword(dataStr);
				try {
					rawhash = Integer.valueOf(dataStr);
				}
				catch (Exception e) {
					
				}
				boolean isFound = dataNode.hashBucket.contains(rawhash) ? true : false;
				String message = dataStr + " (hash value: " + rawhash + ") written to this Data Node " + dataNode.IP + ":" + dataNode.port;
				if (!isFound) {
					message = dataStr + " (hash value: " + rawhash + ") not able to be written to this Data Node " + dataNode.IP + ":" + dataNode.port + ".";
					message += " It can be written to Data Node " + dataNode.findNodeInfo(rawhash);
				}
				if (isFound) {
					dataNode.epoch++;
				}
				return new Response(true, message).serialize();
			}
			else {
				return new Response(false, "Command not supported").serialize();
			}
    	}
    	catch (Exception e) {
    		return new Response(false, e.toString()).serialize();
    	}

    }
}


