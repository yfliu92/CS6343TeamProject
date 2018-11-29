package dht.Ring;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.sql.Timestamp;
import java.util.*;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonWriter;

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
    Document config;
    int port;
    String IP;

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
            config = reader.read(inputFile);
            port = Integer.parseInt(config.getRootElement().element("proxy").element("port").getStringValue());
            IP = config.getRootElement().element("proxy").element("ip").getStringValue();

            // Read the elements in the configuration file
			numOfReplicas = Integer.parseInt(config.getRootElement().element("replicationLevel").getStringValue());
			hashRange = Integer.parseInt(config.getRootElement().element("hashRange").getStringValue());
			vm_to_pm_ratio = Integer.parseInt(config.getRootElement().element("vm_to_pm_ratio").getStringValue());
			total_CCcommands = Integer.parseInt(config.getRootElement().element("total_CCcommands").getStringValue());

			// Get the port
			Element port = config.getRootElement().element("port");
			int startPort = Integer.parseInt(port.element("startPort").getStringValue());
			int portRange = Integer.parseInt(port.element("portRange").getStringValue());

			// Get the IPs
			Element nodes = config.getRootElement().element("nodes");
            List<Element> listOfNodes = nodes.elements();
            int numOfNodes = listOfNodes.size();

            BinarySearchList table = new BinarySearchList();
            HashMap<String, PhysicalNode> physicalNodes = new HashMap<>();

            for (int i = 0; i < numOfNodes; i++){
                String ip = listOfNodes.get(i).element("ip").getStringValue();
				for (int j = 0; j < portRange; j++){
					String nodeID = ip + "-" + (startPort + j) ;
					PhysicalNode node = new PhysicalNode(nodeID, ip, startPort + j, "active");
					physicalNodes.put(nodeID, node);
				}
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
	
	public int initializeDataNode() {
		Element port = config.getRootElement().element("port");
		int startPort = Integer.parseInt(port.element("startPort").getStringValue());
		int portRange = Integer.parseInt(port.element("portRange").getStringValue());
		
		Element nodes = config.getRootElement().element("nodes");
        List<Element> listOfNodes = nodes.elements();
        int numOfNodes = listOfNodes.size();
        int result = 0;
        for (int i = 0; i < numOfNodes; i++){
            String ip = listOfNodes.get(i).element("ip").getStringValue();
//            if (i > 0) {break;}
			for (int j = 0; j < portRange; j++){
//	    		Thread t = new RunDataNode(ip, startPort + j);
//	    		t.start();
				pushDHT(ip, startPort + j);

			}
        }
        
        return result;
	}
	
	public void CCcommands(){
		BufferedWriter writer = null;
		String rootPath = System.getProperty("user.dir");
//		String path = rootPath + File.separator + "src" + File.separator + "dht" + File.separator + "Ring" + File.separator + "ring_CCcommands.txt";

		String path = rootPath + File.separator + "dht" + File.separator + "Ring" + File.separator + "ring_CCcommands.txt";

		try {
			writer = new BufferedWriter(new FileWriter(path), 32768);
			String[] availableCommands = {"add1", "add2", "remove1", "remove2", "loadbalance"};
			String[] availableIPs = {"192.168.0.219","192.168.0.221"};
			String[] availablePorts = {"8101", "8102", "8103", "8104", "8105", "8106", "8107", "8108", "8109", "8110",
					"8111", "8112", "8113", "8114", "8115", "8116", "8117", "8118", "8119", "8120"};
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

			// Write control client commands into the "ring_CCcommands.txt" file
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
						ran_delta = ran.nextInt(100) - 50;
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
	
	public void pushDHTAll() {
		System.out.println("Beginning to push DHT to all physical nodes");
		for(PhysicalNode node: super.getLookupTable().getPhysicalNodeMap().values()) {
			pushDHT(node.getAddress(), node.getPort());
		}
	}
	
	public boolean pushDHT(String serverAddress, int port) {
		try {
		   	ProxyClient_Ring client = new ProxyClient_Ring(this);
	    	boolean connected = client.connectServer(serverAddress, port);
	    	
	    	Thread t = null;
	    	if (!connected) {
	    		t = new RunDataNode_Ring(serverAddress, port);
	    		t.start();
	    		
	    		Thread.sleep(1000);
	    		connected = client.connectServer(serverAddress, port);
	    	}
	    	
	    	Thread.sleep(1000);
			if (connected) {
				
				System.out.println("Connected to Data Node Server at " + serverAddress + ":" + port);
				
				JsonObject jobj = new Response(true, super.getLookupTable().toJSON(), "dht push").toJSON();
				
	        	client.output.println(jobj);
	        	client.output.flush();
				
		        ByteArrayOutputStream baos = new ByteArrayOutputStream();
		        JsonWriter writer = Json.createWriter(baos);
		        writer.writeObject(jobj);
		        writer.close();
		        baos.writeTo(client.outputStream);

		        client.outputStream.write("\n".getBytes());
		        client.outputStream.flush();

		        JsonObject res = parseRequest(client.input);
		        if (res != null) {
		            System.out.println();
		        	System.out.println("Response received at " + new Date() + " ---- " + res.toString());
		            if (res.containsKey("status") && res.containsKey("message")) {
		                System.out.println("REPONSE STATUS: " + res.getString("status") + ", " + "message: " + res.getString("message"));
		            }
		            System.out.println();
		         }
		        
		        client.disconnectServer();
//		        System.out.println("Disconnected with " + serverAddress + ":" + port);
				
				return true;
			}
			else {
//				System.out.println("Unable to connect to Data Node Server at " + serverAddress + " " + port);
				return false;
			}
		}
		catch (Exception e) {
			System.out.println("Exception pushing DHT to Data Node " + serverAddress + " " + port);
			return false;
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
	
	public String getResponse(Command command, DataStore dataStore) {
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
				return super.loadBalance(delta, hash).replaceAll("\n", "  ");
			}
			else if (command.getAction().equals("add")) {
				String ip = command.getCommandSeries().get(0);
				int port = Integer.valueOf(command.getCommandSeries().get(1));
				int hash = command.getCommandSeries().size() == 3 ? Integer.valueOf(command.getCommandSeries().get(2)) : -1;
				int numHashes = command.getCommandSeries().size() - 1;
				int[] hashes = numHashes > 0 ? new int[numHashes] : new int[0];
				
				for(int i = 0; i < numHashes; i++) {
					hashes[i] = Integer.valueOf(command.getCommandSeries().get(i+1));
				}
				
//				String result = hash == -1 ? super.addNode(ip, port) : super.addNode(ip, port, hash);
				String result = hashes.length == 0 ? super.addNode(ip, port) : super.addNode(ip, port, hashes);
				
				return result.replaceAll("\n", "  ");
			}
			else if (command.getAction().equals("remove")) {
				int hash = Integer.valueOf(command.getCommandSeries().get(0));
				String result = super.deleteNode(hash);
				return result.replaceAll("\n", "  ");
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
				else if (operation.equals("push")) {
					if (command.getCommandSeries().size() == 3) {
						String ip = command.getCommandSeries().get(1);
						int port = Integer.valueOf(command.getCommandSeries().get(2));
						pushDHT(ip, port);
						return new Response(true, "DHT pushed for " + ip + " " + port).serialize();
					}
					else if (command.getCommandSeries().size() == 1) {
						return new Response(true, "DHT push to all nodes is being executed").serialize();
					}
					else {
						return new Response(false, "DHT not pushed").serialize();
					}
					
					
				}
				else {
					return new Response(false, "Command not supported").serialize();
				}
			
			}
			else if (command.getAction().equals("run")) {
				String operation = command.getCommandSeries().size() > 0 ? command.getCommandSeries().get(0) : "";
				if (operation.equals("datanode")) {
					int result = initializeDataNode();
					return new Response(true, String.valueOf(result), "A total of " + result + " data nodes are running").serialize();
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
						String requestStr = msg.length() > 200 ? msg.substring(0, 200) + "...": msg; 

                    	System.out.println("Request received from " + s.getPort() + ": " + requestStr + " ---- " + new Date().toString());
                    	System.out.println();
                    	
                    	Command command = new Command(msg);
                    	
                    	String response = proxy.getResponse(command, dataStore);

                    	output.println(response);
                    	output.flush();

						String responseStr = response.length() > 200 ? response.substring(0, 200) + "...": response; 
                    	
                        System.out.println("Response sent to " + s.getPort() + ": " + responseStr + " ---- " + new Date().toString());
                        System.out.println();
                        
                        if (!response.startsWith("false|")) {
                            String[] updateCommands = {"add", "remove", "loadbalance"};
                            for (String cmd: updateCommands) { 
                            	if (command.getAction().equals(cmd)) {
                            		pushDHTAll();
                            		break;
                            	}
                            	else if (command.getAction().equals("dht") && command.getCommandSeries().size() == 1 && command.getCommandSeries().get(0).equals("push")) {
                             		pushDHTAll();
                            		break;
                            	}
                            }
                        }
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
		proxy.initializeDataNode();
		

		//System.out.println(proxy.loadBalance(-13, 320));
		DataStore dataStore = new DataStore();
//		int port = 9091;
		int port = proxy.port;
		String serverAddress = proxy.IP;
        ServerSocket ss = new ServerSocket(port); 
        System.out.println("Ring server running at " + String.valueOf(port));
//        int[] hashes = {38337,72419,61684,51782,61592,63337,81197,72141,17165,12943};
//		System.out.println(proxy.addNode("192.168.0.217", 8005, hashes ));
//		System.out.println(proxy.addNode("192.168.0.203", 8191, 91939 ));
//		System.out.println(proxy.deleteNode(89280 ));
//		System.out.println(proxy.loadBalance(25, 99420 ));
        
        

        while (true)
        { 
            Socket s = null; 
              
            try 
            {
                s = ss.accept(); 
                  
                System.out.println("A new client is connected : " + s); 
                  
            	BufferedReader input = new BufferedReader(new InputStreamReader(s.getInputStream()));
		        PrintWriter output = new PrintWriter(s.getOutputStream(), true);
  
                Thread t = proxy.new ClientHandler(s, input, output, proxy, dataStore); 

                t.start(); 
                  
            } 
            catch (Exception e){ 
                s.close(); 
                e.printStackTrace(); 
            } 
        } 
    }
}

class ProxyClient_Ring{
    PrintWriter output;
    BufferedReader input;
    InputStream inputStream;
    OutputStream outputStream;
    
    Socket socket;
    ProxyServer proxy;
	public ProxyClient_Ring(ProxyServer proxy) { 
		this.proxy = proxy;
	}
	
    public boolean connectServer(String serverAddress, int port) {
    	int timeout = 2000;
		try {
			SocketAddress socketAddress = new InetSocketAddress(serverAddress, port);
			this.socket = new Socket();
			this.socket.connect(socketAddress, timeout);
			this.inputStream = this.socket.getInputStream();
			this.outputStream = this.socket.getOutputStream();
			this.output = new PrintWriter(this.outputStream, true);
			this.input = new BufferedReader(new InputStreamReader(this.inputStream));

	        System.out.println("Connected to server " + serverAddress + ":" + port + ", with local port " + this.socket.getLocalPort());
//			socket.close();
			return true;
 
		} catch (SocketTimeoutException exception) {
//			socket.close();
//			System.out.println("SocketTimeoutException " + serverAddress + ":" + port + ". " + exception.getMessage());
			return false;
		} catch (IOException exception) {
//			socket.close();
//			System.out.println(
//					"IOException - Unable to connect to " + serverAddress + ":" + port + ". " + exception.getMessage());
			return false;
		}
    }
    
    public void disconnectServer() {
    	try {
    		
    		this.input.close();
    		this.output.close();
    		this.socket.close();
    	}
    	catch (Exception e) {
//    		e.printStackTrace();
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
    	if (command.getAction().equals("read")) {
    		if (command.getCommandSeries().size() > 0) {

    			String physicalnodeid_remote = res.getString("result");
    			System.out.println("Physical Node Ids from Remote DHT: " + physicalnodeid_remote);
    			return;
    		}
    	}
    	else if (command.getAction().equals("write")) {
    		
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
        	sendCommandStr_JsonRes(command, input, output);
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

class RunDataNode_Ring extends Thread {
	final String ip;
	final int port;
	public RunDataNode_Ring(String ip, int port) {
		this.ip = ip;
		this.port = port;
	}
	
	@Override
	public void run() {
		runDataNode(this.ip, this.port);
	}
	
	public boolean runDataNode(String ip, int port) {
		boolean success = false;
        try {
        	String dataPortNum = String.valueOf(port);
			Process p = Runtime.getRuntime().exec("java -classpath .:../lib/* dht/Ring/DataNode " + ip + " " + dataPortNum);
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(p.getInputStream()));
			String line = null;
			while ((line = in.readLine()) != null) {
			    System.out.println(line);
			}
			
			System.out.println("Data Node " + p + " " + (p.isAlive() ? "running " + " at " + ip + ":" + dataPortNum : "not running"));
			
			try {
				p.waitFor();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			success = true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        return success;
	}
}
