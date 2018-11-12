package dht.elastic_DHT_centralized;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import dht.common.Hashing;
import dht.common.response.Response;
import dht.server.Command;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.*;

public class ProxyServer extends Proxy {
    public ProxyServer(){
        super();
    }

    public static Proxy initializeEDHT(){
        try {
            // Read from the configuration file "config_ring.xml"
//            String xmlPath = System.getProperty("user.dir") + File.separator + "src" + File.separator + "dht" + File.separator + "elastic_DHT_centralized" + File.separator + "config_ElasticDHT.xml";
            String xmlPath = System.getProperty("user.dir") + File.separator + "dht" + File.separator + "elastic_DHT_centralized" + File.separator + "config_ElasticDHT.xml";

            File inputFile = new File(xmlPath);
            SAXReader reader = new SAXReader();
            Document document = reader.read(inputFile);

            // Read the elements in the configuration file
            Element proxyNode = document.getRootElement().element("proxy");
            String proxyIP = proxyNode.element("ip").getStringValue();
            int proxyPort = Integer.parseInt(proxyNode.element("port").getStringValue());
            // Create the proxy node
            Proxy proxy = new Proxy(proxyIP, proxyPort);

            Element nodes = document.getRootElement().element("nodes");
            List<Element> listOfNodes = nodes.elements();
            int numOfNodes = listOfNodes.size();
            HashMap<Integer, HashMap<String, String>> table = new HashMap<>();
            HashMap<String, PhysicalNode> physicalNodes = new HashMap<>();

            for (int i = 0; i < numOfNodes; i++){
                String ip = listOfNodes.get(i).element("ip").getStringValue();
                int port = Integer.parseInt(listOfNodes.get(i).element("port").getStringValue());
                String nodeID = ip + "-" + Integer.toString(port);
                PhysicalNode node = new PhysicalNode(ip, port, "active");
                physicalNodes.put(nodeID, node);
            }
            // During initialization, hashRange is evenly distributed among the physical nodes
            // If hashRange is 1000 and there are 10 physical nodes in total
            // then the first node gets assigned (0, 99)
            // The second node gets assigned (100, 199)
            // ...
            // The last node gets assigned (900, 999)
            int loadPerNode = HashAndReplicationConfig.HASH_RANGE / physicalNodes.size();
            // Define the start hash value for hash nodes
            int start = 0;
            // Get a list of all physical node ids
            Set<String> idSet = physicalNodes.keySet();
            List<String> idList = new ArrayList<>(idSet);
            Collections.sort(idList);
            int numOfPhysicalNodes = idList.size();

            for (int i = 0; i < numOfPhysicalNodes; i++){
                for (int j = start; j < start + loadPerNode; j++){
                    HashMap<String, String> replicas = new HashMap<>();
                    for (int k = 0; k < HashAndReplicationConfig.REPLICATION_LEVEL; k++) {
                        replicas.put(idList.get((i + k) % numOfPhysicalNodes), idList.get((i + k) % numOfPhysicalNodes));
                    }
                    table.put(j, replicas);

                }
                start += loadPerNode;

            }
            // Create a lookupTable and set it to every physical node
            LookupTable t = new LookupTable();
            t.setBucketsTable(table);
            t.setPhysicalNodesMap(physicalNodes);
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            t.setEpoch(timestamp.getTime());
            proxy.setLookupTable(t);

            for (PhysicalNode node : physicalNodes.values()){
                node.setLookupTable(t);
            }

//            System.out.print("After initialization, the bucketsTable looks as follows: \n");
//            for(int i = 0; i < hashRange; i++) {
//                System.out.print("\nfor hash bucket " + i + ": ");
//                for (String id : table.get(i).keySet()){
//                    System.out.print(id + ", ");
//                }
//
//            }
//            System.out.print("\n");
            
            System.out.println("Initilization success");
            return proxy;

        }catch(DocumentException e) {
        	System.out.println("Initilization failed");
            e.printStackTrace();
            return null;
        }

    }
    
	public static String getFindInfo(String input) {
		return input.toUpperCase();
	}
    
	public String getResponse(String commandStr, Proxy proxy) {
		System.out.println(commandStr);
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

    			String[] physicalnodeids = proxy.getLookupTable().getPhysicalNodeIds(rawhash);
    			return new Response(true, Arrays.toString(physicalnodeids), "Physical Node IDs from server").serialize();
				
			}
			else if (command.getAction().equals("find")) {
				int hash = Integer.valueOf(command.getCommandSeries().get(0));
				return new Response(true, Arrays.toString(proxy.getLookupTable().getPhysicalNodeIds(hash)), "Physical Node Info at Server").serialize();
			}
			else if (command.getAction().equals("loadbalance")) {
				String fromIP = command.getCommandSeries().get(0);
				int fromPort = Integer.valueOf(command.getCommandSeries().get(1));
				String toIP = command.getCommandSeries().get(2);
				int toPort = Integer.valueOf(command.getCommandSeries().get(3));
				int numBuckets = Integer.valueOf(command.getCommandSeries().get(4));
				return proxy.loadBalance(fromIP, fromPort, toIP, toPort, numBuckets);
			}
			else if (command.getAction().equals("add")) {
				String ip = command.getCommandSeries().get(0);
				int port = Integer.valueOf(command.getCommandSeries().get(1));
				int start = command.getCommandSeries().size() == 4 ? Integer.valueOf(command.getCommandSeries().get(2)) : -1;
				int end = command.getCommandSeries().size() == 4 ? Integer.valueOf(command.getCommandSeries().get(3)) : -1;
				
				String result = start == -1 && end == -1 ? proxy.addNode(ip, port) : proxy.addNode(ip, port, start, end);
				return result;
			}
			else if (command.getAction().equals("remove")) {
				String IP = command.getCommandSeries().get(0);
				int port = Integer.valueOf(command.getCommandSeries().get(1));
				String result = proxy.deleteNode(IP, port);
				return result;
//				return "remove";
			}
			else if (command.getAction().equals("info")) {
//				return proxy.listNodes();
				return new Response(true, proxy.getLookupTable().toJSON(), "DHT Table from Server").serialize();
			}
			else if (command.getAction().equals("dht")) {
				String operation = command.getCommandSeries().size() > 0 ? command.getCommandSeries().get(0) : "head";
				if (operation.equals("head")) {
					return new Response(true, String.valueOf(proxy.getLookupTable().getEpoch()), "Current epoch number:").serialize();
				}
				else if (operation.equals("pull")) {
//					return super.getLookupTable().serialize();
					return new Response(true, proxy.getLookupTable().toJSON(), "Elastic DHT table").serialize();
				}
				else if (operation.equals("print")) {
					proxy.getLookupTable().print();
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
			ee.printStackTrace();
			return "Illegal command";
		}

	}
	
	public class ClientHandler extends Thread  
	{
	    final BufferedReader input;
	    final PrintWriter output;
	    final Socket s; 
	    final ProxyServer proxyServer;
	    final Proxy proxy;
	  
	    public ClientHandler(Socket s, BufferedReader input, PrintWriter output, ProxyServer proxyServer, Proxy proxy) {
	    	this.s = s; 
	    	this.input = input;
	        this.output = output;
	        this.proxy = proxy;
	        this.proxyServer = proxyServer;
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
                    	
                    	String response = proxyServer.getResponse(msg, proxy);

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
		ProxyServer proxyServer = new ProxyServer();
        //Initialize the Elastic DHT cluster
	    Proxy proxy = initializeEDHT();
	    System.out.println(proxy.addNode("192.168.0.211", 8100, 900, 910));
		int port = 9093;
        ServerSocket ss = new ServerSocket(port); 
        System.out.println("Elastic DHT server running at " + String.valueOf(port));
          
        while (true)  
        { 
            Socket s = null; 
              
            try 
            {
                s = ss.accept(); 
                  
                System.out.println("A new client is connected : " + s); 
                  
            	BufferedReader input = new BufferedReader(new InputStreamReader(s.getInputStream()));
		        PrintWriter output = new PrintWriter(s.getOutputStream(), true);

                Thread t = proxyServer.new ClientHandler(s, input, output, proxyServer, proxy); 

                t.start(); 
                  
            } 
            catch (Exception e){ 
                s.close(); 
                e.printStackTrace(); 
            } 
        } 
    }
    
//    public static void main(String[] args) throws IOException {
//        ProxyServer proxyServer = new ProxyServer();
//        //Initialize the Elastic DHT cluster
//        Proxy proxy = initializeEDHT();
//        System.out.println(proxy.addNode("192.168.0.211", 8100, 900, 910));
////        System.out.println(proxy.deleteNode("192.168.0.201", 8100));
////        System.out.println(proxy.loadBalance("192.168.0.204", 8100, "192.168.0.210", 8100, 12));
//
//        int port = 9093;
//    	System.out.println("Elastic DHT server running at " + String.valueOf(port));
//        ServerSocket listener = new ServerSocket(port);
//
//        try {
//            while (true) {
//            	Socket socket = null;
//                try {
//                	socket = listener.accept();
//                	System.out.println("Connection accepted: " + socket + " ---- " + new Date().toString());
//                	
//                	BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
//    		        PrintWriter output = new PrintWriter(socket.getOutputStream(), true);
//                	
//                    Thread t = proxyServer.new ClientHandler(socket, input, output, proxyServer); 
//
//                    t.start(); 
//                	
////                	BufferedReader in = new BufferedReader(
////                            new InputStreamReader(socket.getInputStream()));
////                    PrintWriter out =
////                            new PrintWriter(socket.getOutputStream(), true);
////                	String msg;
////                	while(true) {
////                		try {
////                    		msg = in.readLine();
////                        	if (msg != null) {
////                            	System.out.println("Request received: " + msg + " ---- " + new Date().toString());
////
////                                String response = proxyServer.getResponse(msg, proxy);
////                                out.println(response);
////                                System.out.println("Response sent: " + response);
////                        	}
////                        	else {
////                        		System.out.println("Connection end " + " ---- " + new Date().toString());
////                        		break;
////                        	}
////                		}
////                		catch (Exception ee) {
////                    		System.out.println("Connection reset " + " ---- " + new Date().toString());
////                    		break;
////                		}
////
////                	}
//
//                } finally {
//                    socket.close();
//                }
//            }
//        }
//        finally {
//            listener.close();
//        }
//        
//    }
}
