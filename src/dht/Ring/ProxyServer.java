package dht.Ring;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import dht.common.Hashing;
import dht.server.Command;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import dht.common.response.Response;

public class ProxyServer extends PhysicalNode {
    public static int numOfReplicas = 3;
    public static final int WRITE_BATCH_SIZE = 1000;

	public ProxyServer(){
		super();
	}

	public void initializeRing(){
        try {
            // Read from the configuration file "config_ring.xml"
            String xmlPath = System.getProperty("user.dir") + File.separator + "dht" + File.separator + "Ring" + File.separator + "config_ring.xml";

//            String xmlPath = System.getProperty("user.dir") + File.separator + "src" + File.separator + "dht" + File.separator + "Ring" + File.separator + "config_ring.xml";
            System.out.println(xmlPath);
            File inputFile = new File(xmlPath);
            SAXReader reader = new SAXReader();
            Document document = reader.read(inputFile);

            // Read the elements in the configuration file
            numOfReplicas = Integer.parseInt(document.getRootElement().element("replicationLevel").getStringValue());
            int hashRange = Integer.parseInt(document.getRootElement().element("hashRange").getStringValue());
            int virtualNodesMapping = Integer.parseInt(document.getRootElement().element("virtualNodesMapping").getStringValue());
            Element nodes = document.getRootElement().element("nodes");
            List<Element> listOfNodes = nodes.elements();
            int numOfNodes = listOfNodes.size();

            BinarySearchList table = new BinarySearchList();
            HashMap<String, PhysicalNode> physicalNodes = new HashMap<>();

            for (int i = 0; i < numOfNodes; i++){
                String ip = listOfNodes.get(i).element("ip").getStringValue();
                int port = Integer.parseInt(listOfNodes.get(i).element("port").getStringValue());
                String nodeID = ip + "-" + Integer.toString(port);
                PhysicalNode node = new PhysicalNode(nodeID, ip, port, "active");
                physicalNodes.put(nodeID, node);
            }
            // If hashRange is 10000 and there are 10 physical nodes in total, then stepSize is 1000
            // The first physical node will start from 0 and map to virtual nodes of hash 0, 1000, 2000,...,9000
            // The second physical node will start from 100 and map to virtual nodes of hash 100, 1100, 2100,...,9100
            // ...
            // The last physical node will start from 900 and map to virtual nodes of hash 900, 1900, 2900,...,9900
            int stepSize = hashRange / physicalNodes.size();
            // Define the start hash value for hash nodes
            int start = 0;
            for (String id : physicalNodes.keySet()){
                List<VirtualNode> virtualNodes = new ArrayList<>();
                // Each physical node maps to 10 virtual nodes during initialization
                for (int i = start; i < hashRange; i += stepSize){
                    VirtualNode vNode = new VirtualNode(i, id);
                    virtualNodes.add(vNode);
                    table.add(vNode);
                }
                physicalNodes.get(id).setVirtualNodes(virtualNodes);
                start += hashRange / (physicalNodes.size() * virtualNodesMapping);
            }
            
            table.updateIndex();
            
            // Create a lookupTable and set it to every physical node
            LookupTable t = new LookupTable();
            t.setTable(table);
            t.setPhysicalNodeMap(physicalNodes);
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            t.setEpoch(timestamp.getTime());

            for (PhysicalNode node : physicalNodes.values()){
                node.setLookupTable(t);
            }
            
            super.setLookupTable(t);

//            String result = "After initialization, virtual nodes include: \r\n";
//            for(VirtualNode node : t.getTable()) {
//                result += node.getHash() + " ";
//            }
//
//            result += "physical node IDS: ";
//            for (String id : t.getPhysicalNodeMap().keySet()){
//                result += id + ", ";
//            }
//            System.out.print(result);
            
			System.out.println("Initialized successfully: " + physicalNodes.values().size() + " physical nodes, " + t.getTable().size() + " virtual nodes");
        }catch(DocumentException e) {
        	System.out.println("Failed to initialize");
            e.printStackTrace();
        }
    }
	
	public String getResponse(String commandStr, DataStore dataStore) {
		Command command = new Command(commandStr);
		try {
			if (command.getAction().equals("read")) {
				String dataStr = command.getCommandSeries().get(0);
//				return dataStore.readRes(dataStr);
				
    			int rawhash = Hashing.getHashValFromKeyword(dataStr);
    			try {
    				rawhash = Integer.valueOf(dataStr);
    			}
    			catch (Exception e) {
    				
    			}

    			int[] virtualnodeids = super.getLookupTable().getTable().getVirtualNodeIds(rawhash);
    			return new Response(true, Arrays.toString(virtualnodeids), "Virtual Node IDs from server").serialize();
				
			}
			else if (command.getAction().equals("write")) {
				String dataStr = command.getCommandSeries().get(0);
				int rawhash = Hashing.getHashValFromKeyword(dataStr);
				int[] virtualnodeids = super.getLookupTable().getTable().getVirtualNodeIds(rawhash);
				
				return dataStore.writeRes(dataStr, rawhash, virtualnodeids);
			}
			else if (command.getAction().equals("writebatch")) {
				int batchsize = command.getCommandSeries().size() > 0 ? Integer.valueOf(command.getCommandSeries().get(0)) : WRITE_BATCH_SIZE;
				return dataStore.writeRandomRes(batchsize, this);
			}
			else if (command.getAction().equals("updatebatch")) {
				int updateNo = command.getCommandSeries().size() > 0 ? Integer.valueOf(command.getCommandSeries().get(0)) : 100;
				return dataStore.updateRandomRes(updateNo, this);
			}
			else if (command.getAction().equals("data")) {
				return dataStore.listDataRes();
			}
			else if (command.getAction().equals("loadbalance")) {
				int delta = Integer.valueOf(command.getCommandSeries().get(0));
				int hash = Integer.valueOf(command.getCommandSeries().get(1));
				return super.loadBalance(delta, hash);
			}
			else if (command.getAction().equals("add")) {
				String ip = command.getCommandSeries().get(0);
				int port = Integer.valueOf(command.getCommandSeries().get(1));
				int hash = command.getCommandSeries().size() == 3 ? Integer.valueOf(command.getCommandSeries().get(2)) : -1;
				String result = hash == -1 ? super.addNode(ip, port) : super.addNode(ip, port, hash);
				return result;
			}
			else if (command.getAction().equals("remove")) {
				int hash = Integer.valueOf(command.getCommandSeries().get(0));
				String result = super.deleteNode(hash);
				return result;
			}
			else if (command.getAction().equals("find")) {
				int hash = Integer.valueOf(command.getCommandSeries().get(0));
				return new Response(true, super.getLookupTable().getTable().find(hash).toJSON(), "Virtual Node Info at Server").serialize();
			}
			else if (command.getAction().equals("info")) {
//				return new Response(true, super.listNodes()).serialize();
				return new Response(true, super.getLookupTable().toJSON(), "DHT Table from Server").serialize();
			}
			else if (command.getAction().equals("dht")) {
				String operation = command.getCommandSeries().size() > 0 ? command.getCommandSeries().get(0) : "head";
				if (operation.equals("head")) {
					return new Response(true, String.valueOf(super.getLookupTable().getEpoch()), "Current epoch number:").serialize();
				}
				else if (operation.equals("pull")) {
//					return super.getLookupTable().serialize();
					return new Response(true, super.getLookupTable().toJSON(), "Ring DHT table").serialize();
				}
				else if (operation.equals("print")) {
					super.getLookupTable().print();
					return new Response(true, "DHT printed on server").serialize();
				}
				else {
					return new Response(false, "Command not supported").serialize();
				}
			
			}
			else {
				return "Command not supported";
			}
		}
		catch (Exception ee) {
			return "Illegal command " + ee.toString();
		}
	}
	
	public class ClientHandler extends Thread  
	{
	    final BufferedReader input;
	    final PrintWriter output;
	    final Socket s; 
	    final ProxyServer proxy;
	    final DataStore dataStore;
	  
	    public ClientHandler(Socket s, BufferedReader input, PrintWriter output, ProxyServer proxy, DataStore dataStore) {
	    	this.s = s; 
	    	this.input = input;
	        this.output = output;
	        this.proxy = proxy;
	        this.dataStore = dataStore;
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
                    	
                    	String response = proxy.getResponse(msg, dataStore);

                    	output.println(response);
                    	output.flush();
                    	
                        System.out.println("Response sent to " + s.getPort() + ": " + response + " ---- " + new Date().toString());
                        System.out.println();
                	}
              
	            } catch (IOException e) { 
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
	}
	
    public static void main(String[] args) throws IOException { 
		ProxyServer proxy = new ProxyServer();
		//Initialize the ring cluster
		proxy.initializeRing();
		
		DataStore dataStore = new DataStore();
		int port = 9091;
        ServerSocket ss = new ServerSocket(port); 
        System.out.println("Ring server running at " + String.valueOf(port));
          
        while (true)  
        { 
            Socket s = null; 
              
            try 
            {
                s = ss.accept(); 
                  
                System.out.println("A new client is connected : " + s); 
                  
            	BufferedReader input = new BufferedReader(new InputStreamReader(s.getInputStream()));
		        PrintWriter output = new PrintWriter(s.getOutputStream(), true);
//		        
//            	output.println(s.getPort());
//            	output.flush();
  
                Thread t = proxy.new ClientHandler(s, input, output, proxy, dataStore); 

                t.start(); 
                  
            } 
            catch (Exception e){ 
                s.close(); 
                e.printStackTrace(); 
            } 
        } 
    } 
    
//	public static void main(String[] args) throws IOException {
//	// TODO Auto-generated method stub
//	ProxyServer proxy = new ProxyServer();
//	//Initialize the ring cluster
//	proxy.initializeRing();
//	
//	DataStore dataStore = new DataStore();
////	System.out.println(dataStore.writeRandomRes(WRITE_BATCH_SIZE, proxy));
////	System.out.println(dataStore.updateRandomRes(WRITE_BATCH_SIZE * 3, proxy));
//	
//	int port = 9091;
//	System.out.println("Ring server running at " + String.valueOf(port));
//    ServerSocket listener = new ServerSocket(port);
//    
//    try {
//        while (true) {
//        	Socket socket = listener.accept();
//        	System.out.println("Connection accepted" + " ---- " + new Date().toString());
//            try {
//            	BufferedReader input = new BufferedReader(
//                        new InputStreamReader(socket.getInputStream()));
//                PrintWriter output =
//                        new PrintWriter(socket.getOutputStream(), true);
//            	String msg;
//            	while(true) {
//            		try {
//                		msg = input.readLine();
//                    	if (msg != null) {
//                        	System.out.println("Request received: " + msg + " ---- " + new Date().toString());
//                        	System.out.println();
//
//                            String response = proxy.getResponse(msg, dataStore);
//                            
//                            output.println(response);
//                            System.out.println("Response sent: " + response + " ---- " + new Date().toString());
//                            System.out.println();
//                    	}
//                    	else {
//                    		System.out.println("Connection end " + " ---- " + new Date().toString());
//                    		System.out.println();
//                    		break;
//                    	}
//            		}
//            		catch (Exception ee) {
//                		System.out.println("Connection reset " + " ---- " + new Date().toString());
//                		System.out.println();
//                		break;
//            		}
//
//            	}
//
//            } finally {
//                socket.close();
//            }
//        }
//    }
//    finally {
//        listener.close();
//    }
//	
//}

}
