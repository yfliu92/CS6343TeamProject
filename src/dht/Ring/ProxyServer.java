package dht.Ring;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.*;

import dht.common.Hashing;
import dht.server.Command;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import dht.common.response.Response;

public class ProxyServer extends PhysicalNode {
	public static int numOfReplicas;
	public static int hashRange;
	public static int vm_to_pm_ratio;
	public static int total_CCcommands;

    public static final int WRITE_BATCH_SIZE = 1000;

	public ProxyServer(){
		super();
	}

	public void initializeRing(){
        try {
            // Read from the configuration file "config_ring.xml"
//            String xmlPath = System.getProperty("user.dir") + File.separator + "dht" + File.separator + "Ring" + File.separator + "config_ring.xml";

            String xmlPath = System.getProperty("user.dir") + File.separator + "src" + File.separator + "dht" + File.separator + "Ring" + File.separator + "config_ring.xml";
            System.out.println(xmlPath);
            File inputFile = new File(xmlPath);
            SAXReader reader = new SAXReader();
            Document document = reader.read(inputFile);

            // Read the elements in the configuration file
			numOfReplicas = Integer.parseInt(document.getRootElement().element("replicationLevel").getStringValue());
			hashRange = Integer.parseInt(document.getRootElement().element("hashRange").getStringValue());
			vm_to_pm_ratio = Integer.parseInt(document.getRootElement().element("vm_to_pm_ratio").getStringValue());
			total_CCcommands = Integer.parseInt(document.getRootElement().element("total_CCcommands").getStringValue());
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
            // If hashRange is 1000 and there are 10 physical nodes in total, then stepSize is 100
            // The first physical node will start from 0 and map to virtual nodes of hash 0, 100, 200,...,900
            // The second physical node will start from 100 and map to virtual nodes of hash 10, 110, 210,...,910
            // ...
            // The last physical node will start from 900 and map to virtual nodes of hash 90, 190, 290,...,990
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
                start += hashRange / (physicalNodes.size() * vm_to_pm_ratio);
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
	public void CCcommands(){
		Writer writer = null;

		try {
			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream("ring_CCcommands.txt"), "utf-8"));
			String[] availableCommands = {"add1", "add2", "remove1", "remove2", "loadbalance"};
			String[] availableIPs = {"192.168.0.211","192.168.0.212","192.168.0.213","192.168.0.214",
					"192.168.0.215","192.168.0.216","192.168.0.217","192.168.0.218","192.168.0.219","192.168.0.220",
					"192.168.0.221", "192.168.0.222","192.168.0.223","192.168.0.224","192.168.0.225",
					"192.168.0.226", "192.168.0.227","192.168.0.228","192.168.0.229","192.168.0.230"};
			String[] availablePorts = {"8001", "8002", "8003", "8004", "8005"};
			ArrayList<String> availablePNodes = new ArrayList<>();
			for (String ip : availableIPs){
				for (String port : availablePorts){
					availablePNodes.add(ip + " " + port);
				}
			}
			// Get the current physicalIDs after initialization
			Collection<PhysicalNode> pNodes = super.getLookupTable().getPhysicalNodeMap().values();
			List<PhysicalNode> currentPNodes = new ArrayList<>();
			for (PhysicalNode node : pNodes){
				currentPNodes.add(node);
			}
			List<Integer> currentVNodes = new ArrayList<>();
			for(VirtualNode node : super.getLookupTable().getTable()){
				currentVNodes.add(node.getHash());
			}

			// Write control client commands into the "ring_CCcommands.txt" file (in the "/src" folder by default)
			for (int i = 0; i < total_CCcommands; i++){
				// Randomly pick a command from available commands
				Random ran = new Random();
				String command = availableCommands[ran.nextInt(availableCommands.length)];

				// add1 means to add a virtual node for an existing physical node
				// Use addNode(String ip, int port, int hash) for this command
				if (command.equals("add1")){
					if (currentPNodes.size() == 0){
						continue;
					}
					String ip_port = currentPNodes.get(ran.nextInt(currentPNodes.size())).getId();
					String[] lst = ip_port.split("-");
					int ran_hash;
					do {
						ran_hash = ran.nextInt(hashRange);
					} while (currentVNodes.contains(ran_hash));
					currentVNodes.add(ran_hash);
					writer.write("add " + lst[0] + " " + lst[1] + " " + ran_hash + "\n");
				}

				// add2 means to add a new physical node and map it to more than 1 virtual node
				// Use addNode(String ip, int port, int[] hashes) for this command
				else if (command.equals("add2")){
					if (availablePNodes.size() == 0){
						continue;
					}
					String ip_port = availablePNodes.get(ran.nextInt(availablePNodes.size()));
					String[] lst = ip_port.split(" ");
					String ip = lst[0];
					int port = Integer.parseInt(lst[1]);
					availablePNodes.remove(ip_port);
					Set<Integer> hashes = new HashSet<>();
					while (hashes.size() < vm_to_pm_ratio) {
						int ran_hash;
						do {
							ran_hash = ran.nextInt(hashRange);
						} while (currentVNodes.contains(ran_hash));
						hashes.add(ran_hash);
						currentVNodes.add(ran_hash);

					}
					List<VirtualNode> virtualNodes = new ArrayList<>();
					String vNodes_to_add = "";
					for (Integer hash : hashes){
						VirtualNode vNode = new VirtualNode(hash);
						virtualNodes.add(vNode);
						vNodes_to_add += " " + hash;
					}
					PhysicalNode newNode = new PhysicalNode(ip + "-" + port, ip, port, "active");
					newNode.setVirtualNodes(virtualNodes);
					currentPNodes.add(newNode);
					writer.write("add " + ip_port + vNodes_to_add + "\n");
				}

				// remove1 means to remove a virtual node
				// use deleteNode(int hash) for this command
				else if (command.equals("remove1")){
					if (currentVNodes.size() == 0){
						continue;
					}
					int ran_hash = currentVNodes.get(ran.nextInt(currentVNodes.size())) ;
					currentVNodes.remove(Integer.valueOf(ran_hash));
					writer.write("remove " + ran_hash + "\n");
				}

				// remove2 means to remove a physical node and all its corresponding virutal nodes
				// use failNode(String ip, int port) for this command
				else if (command.equals("remove2")){
					if (currentPNodes.size() == 0){
						continue;
					}
					PhysicalNode node_to_delete = currentPNodes.get(ran.nextInt(currentPNodes.size()));
					String[] lst = node_to_delete.getId().split("-");
					String id = lst[0] + " " + lst[1];
					List<VirtualNode> vNodes = node_to_delete.getVirtualNodes();
					for (VirtualNode vNode : vNodes){
						currentVNodes.remove(Integer.valueOf(vNode.getHash()));
					}
					currentPNodes.remove(node_to_delete);
					availablePNodes.add(id);
					writer.write("remove " + id + "\n");
				}
				// use loadBalance(int delta, int hash) for this command
				else if (command.equals("loadbalance")) {
					if (currentVNodes.size() == 0){
						continue;
					}
					int ran_hash = currentVNodes.get(ran.nextInt(currentVNodes.size())) ;
					int ran_delta;
					do {
						ran_delta = ran.nextInt(200) - 100;
					} while (ran_delta == 0);

					writer.write("loadbalance " + ran_delta + " " + ran_hash + "\n");
				}
			}
		} catch (IOException ex) {
			// Report
		} finally {
			try {writer.close();} catch (Exception ex) {/*ignore*/}
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
		proxy.CCcommands();
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
