package dht.rush;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;

import javax.json.*;

import dht.common.Hashing;
import dht.common.response.Response;
import dht.rush.clusters.Cluster;
import dht.rush.clusters.ClusterStructureMap;
import dht.server.Command;

public class DataNode {
    private ClusterStructureMap clusterStructureMap;
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
		if (clusterStructureMap == null) {
			System.out.println("Local DHT table is not initialized");
			return;
		}
		clusterStructureMap.print();
	}
	
	public boolean buildTable(JsonObject data) {
		clusterStructureMap = new ClusterStructureMap();
		hashBucket = new HashSet<Integer>();
		boolean result = clusterStructureMap.buildTable(data);
		buildHashBucket(clusterStructureMap);
		return result;
	}
	
	public void buildHashBucket(ClusterStructureMap clusterStructureMap) {
		if (clusterStructureMap != null) {
			for(Map.Entry<String, Cluster> child: clusterStructureMap.getChildrenList().entrySet()) {
				Cluster cluster = child.getValue();
	        	
	        	buildHashBucket(cluster);
			}
		}
	}
	
	public void buildHashBucket(Cluster cluster) {
    	if (!cluster.getId().equals("R") && !cluster.getPort().equals("")) {		
    		String IP = cluster.getIp();
    		int port = Integer.valueOf(cluster.getPort());
    		
			if (port == this.port && IP.equals(this.IP)) 
			{
				for(Map.Entry<String, Integer> hashInfo : cluster.getPlacementGroupMap().entrySet()) {
					hashBucket.add(Integer.valueOf(hashInfo.getKey().replaceAll("PG", "")));
				}
			}
    	}
    	
    	if (cluster.getSubClusters() != null) {
    		for(Cluster child: cluster.getSubClusters()) {
    			buildHashBucket(child);
    		}
    	}
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
	
	public int getDataEpoch() {
		return this.epoch;
	}
	
	public String getHashBucket() {
		return Arrays.toString(this.hashBucket.toArray());
	}
	
	public String findNodeInfo(int rawhash) {
		StringBuilder info = new StringBuilder();
		String pgid = "PG" + rawhash;
		for(Map.Entry<String, Cluster> root: clusterStructureMap.getChildrenList().entrySet()) {
			if (root.getKey().equals("R")) {
				Cluster cluster = root.getValue();
				findNodeInfo(cluster, pgid, info);
			}
		}
		
		
		return info.toString();
	}
    
	public void findNodeInfo(Cluster cluster, String pgid, StringBuilder info) {
		if (cluster == null) {
			return;
		}
		System.out.println("cluster " + cluster.getId());
		System.out.println(cluster.toJSON().toString());
		Map<String, Integer> hashMap = cluster.getPlacementGroupMap();
		if (hashMap.get(pgid) != null) {
			info.append(cluster.getIp() + ":" + cluster.getPort());
			info.append("; ");
		}
		
		if (cluster.getSubClusters() != null) {
			for(Cluster child: cluster.getSubClusters()) {
				findNodeInfo(child, pgid, info);
			}
		}
	}
    
    public static void main(String[] args) throws Exception {
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
                  
                System.out.println("A new client is connected to Data Node: " + s); 
                  
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
				return new Response(false, "Command not supported by Data Node").serialize();
			}
    	}
    	catch (Exception e) {
    		return new Response(false, e.toString()).serialize();
    	}

    }
}
