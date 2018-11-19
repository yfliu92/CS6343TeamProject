package dht.elastic_DHT_centralized;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import dht.common.Hashing;
import dht.common.response.Response;
import dht.server.Command;

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

public class ProxyServer extends Proxy {
    public static int REPLICATION_LEVEL = 3;
    public static int INITIAL_HASH_RANGE = 1000;
    public static int CURRENT_HASH_RANGE = 1000;
    public static int TOTAL_CCCOMMANDS = 100;
    public static int LOAD_PER_LOAD = 10;
    
    static Document config;

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
            config = reader.read(inputFile);

            // Read the elements in the configuration file
            Element rootElement = config.getRootElement();
            Element proxyNode = rootElement.element("proxy");
            String proxyIP = proxyNode.element("ip").getStringValue();
            int proxyPort = Integer.parseInt(proxyNode.element("port").getStringValue());
            // Create the proxy node
            Proxy proxy = new Proxy(proxyIP, proxyPort);
            // Get other parameters
            REPLICATION_LEVEL = Integer.parseInt(rootElement.element("replication_level").getStringValue());
            INITIAL_HASH_RANGE = Integer.parseInt(rootElement.element("initial_hash_range").getStringValue());
            CURRENT_HASH_RANGE = INITIAL_HASH_RANGE;
            TOTAL_CCCOMMANDS = Integer.parseInt(rootElement.element("total_CCcommands").getStringValue());
            LOAD_PER_LOAD = Integer.parseInt(rootElement.element("loadPerNode").getStringValue());
            // Get the port
            Element port = rootElement.element("port");
            int startPort = Integer.parseInt(port.element("startPort").getStringValue());
            int portRange = Integer.parseInt(port.element("portRange").getStringValue());

            // Get the IPs
            Element nodes = rootElement.element("nodes");
            List<Element> listOfNodes = nodes.elements();
            int numOfNodes = listOfNodes.size();
            HashMap<Integer, HashMap<String, String>> table = new HashMap<>();
            HashMap<String, PhysicalNode> physicalNodes = new HashMap<>();

            for (int i = 0; i < numOfNodes; i++){
                String ip = listOfNodes.get(i).element("ip").getStringValue();
                for (int j = 0; j < portRange; j++){
                    String nodeID = ip + "-" + (startPort + j) ;
                    PhysicalNode node = new PhysicalNode(ip, startPort + j, "active");
                    physicalNodes.put(nodeID, node);
                }
            }
            // During initialization, hashRange is evenly distributed among the physical nodes
            // If hashRange is 1000 and there are 10 physical nodes in total
            // then the first node gets assigned (0, 99)
            // The second node gets assigned (100, 199)
            // ...
            // The last node gets assigned (900, 999)
            int loadPerNode = INITIAL_HASH_RANGE / physicalNodes.size();
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
                    for (int k = 0; k < REPLICATION_LEVEL; k++) {
                        replicas.put(idList.get((i + k) % numOfPhysicalNodes), idList.get((i + k) % numOfPhysicalNodes));
                        physicalNodes.get(idList.get((i + k) % numOfPhysicalNodes)).getHashBuckets().add(j);
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

            System.out.println("Initialization succeeded");
            System.out.println("Buckets table size " + table.size());
            System.out.println("Physical nodes size " + physicalNodes.size());

            return proxy;

        }catch(DocumentException e) {
            System.out.println("Initialization failed");
            e.printStackTrace();
            return null;
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
            if (i > 0) {break;}
			for (int j = 0; j < portRange; j++){
//	    		Thread t = new RunDataNode(ip, startPort + j);
//	    		t.start();
				pushDHT(ip, startPort + j);

			}
        }
        
        return result;
	}
    
    public void CCcommands(Proxy proxy) {
        BufferedWriter writer = null;
        String rootPath = System.getProperty("user.dir");
//        String path = rootPath + File.separator + "src" + File.separator + "dht" + File.separator + "elastic_DHT_centralized" + File.separator + "elastic_CCcommands.txt";
        String path = rootPath + File.separator + "dht" + File.separator + "elastic_DHT_centralized" + File.separator + "elastic_CCcommands.txt";
		System.out.println("writing to file");
        try {
            writer = new BufferedWriter(new FileWriter(path), 32768);
            String[] availableCommands = {"add", "remove", "loadbalance"};
            String[] expand_shrink_commands = {"expand", "shrink"};
            String[] availableIPs = {"192.168.0.219","192.168.0.221"};
            String[] availablePorts = {"8101", "8102", "8103", "8104", "8105", "8106", "8107", "8108", "8109", "8110",
                    "8111", "8112", "8113", "8114", "8115", "8116", "8117", "8118", "8119", "8120"};
            ArrayList<String> availablePNodes = new ArrayList<>();
            for (String ip : availableIPs) {
                for (String port : availablePorts) {
                    availablePNodes.add(ip + " " + port);
                }
            }
            // Get the current physicalIDs after initialization
            Set<String> pNodes = proxy.getLookupTable().getPhysicalNodesMap().keySet();
            List<String> currentPNodes = new ArrayList<>();
            for (String node : pNodes){
                String[] lst = node.split("-");
                currentPNodes.add(lst[0] + " " + lst[1]);
            }
            // Write control client commands into the "elastic_CCcommands.txt" file (in the root folder by default)
            for (int i = 0; i < TOTAL_CCCOMMANDS; i++){
                Random ran = new Random();
                // Randomly pick a command between "expand" and "shrink" when i is 99, 199, 299....
                if (i % 200 == 199){
                    String command = expand_shrink_commands[ran.nextInt(expand_shrink_commands.length)];
                    writer.write(command + "\n");
                    continue;
                }
                // Randomly pick a command from available commands
                String command = availableCommands[ran.nextInt(availableCommands.length)];

                // add means to add a physical node and specify for what range of buckets it will serve as a replica
                // Use addNode(String ip, int port, int start, int end)
                if (command.equals("add")){
                    if (availablePNodes.size() == 0){
                        continue;
                    }
                    String ip_port = availablePNodes.get(ran.nextInt(availablePNodes.size()));
                    availablePNodes.remove(ip_port);
                    currentPNodes.add(ip_port);
                    int ran_start = ran.nextInt(INITIAL_HASH_RANGE);
                    int ran_end = (ran_start + LOAD_PER_LOAD) % INITIAL_HASH_RANGE;
                    writer.write("add " + ip_port + " " + ran_start + " " + ran_end + "\n");
                }
                // remove means to remove a physical node
                // use deleteNode(String ip, int port) for this command
                else if (command.equals("remove")){
                    if (currentPNodes.size() == 0){
                        continue;
                    }
                    String ip_port = currentPNodes.get(ran.nextInt(currentPNodes.size()));
                    currentPNodes.remove(ip_port);
                    availablePNodes.add(ip_port);
                    writer.write("remove " + ip_port + "\n");
                }
                // use loadBalance(String fromID, String toID, int numOfBuckets) for this command
                else if (command.equals("loadbalance")) {
                    if (currentPNodes.size() <= 1){
                        continue;
                    }
                    String fromID = currentPNodes.get(ran.nextInt(currentPNodes.size()));
                    String toID;
                    do {
                        toID = currentPNodes.get(ran.nextInt(currentPNodes.size()));
                    } while (toID == fromID);

                    int numOfBuckets = ran.nextInt(15) + 10;
                    writer.write("loadbalance " + fromID + " " + toID + " " + numOfBuckets + "\n");
                }
            }
        } catch (IOException ex) {
            // Report
        } finally {
            try {writer.close();}
            catch (Exception ex) {/*ignore*/}
        }

    }

    public static String getFindInfo(String input) {
		return input.toUpperCase();
	}
    
	public boolean pushDHT(String serverAddress, int port) {
		try {
			ProxyClient_Elastic client = new ProxyClient_Elastic(this);
	    	boolean connected = client.connectServer(serverAddress, port);
	    	
	    	Thread t = null;
	    	if (!connected) {
	    		t = new RunDataNode_Elastic(serverAddress, port);
	    		t.start();
	    		
	    		Thread.sleep(1000);
	    		connected = client.connectServer(serverAddress, port);
	    	}
	    	
	    	Thread.sleep(1000);
			if (connected) {
				
				System.out.println("Connected to Data Node Server at " + serverAddress + " " + port);
				
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
				
//		        Thread.sleep(3000);
//				client.processCommand(1, "dht push");
//				client.processCommand(1, "exit");
//				client.socket.close();
//				System.out.println("Disconnected to Data Node Server at " + serverAddress + " " + port);
				
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
				
				pushDHT(fromIP, fromPort);
				pushDHT(toIP, toPort);
				
				return proxy.loadBalance(fromIP, fromPort, toIP, toPort, numBuckets).replaceAll("\n", "  ");
			}
			else if (command.getAction().equals("add")) {
				String ip = command.getCommandSeries().get(0);
				int port = Integer.valueOf(command.getCommandSeries().get(1));
				int start = command.getCommandSeries().size() == 4 ? Integer.valueOf(command.getCommandSeries().get(2)) : -1;
				int end = command.getCommandSeries().size() == 4 ? Integer.valueOf(command.getCommandSeries().get(3)) : -1;
				
				String result = start == -1 && end == -1 ? proxy.addNode(ip, port) : proxy.addNode(ip, port, start, end);
				
				pushDHT(ip, port);
				
				return result.replaceAll("\n", "  ");
			}
			else if (command.getAction().equals("remove")) {
				String IP = command.getCommandSeries().get(0);
				int port = Integer.valueOf(command.getCommandSeries().get(1));
				String result = proxy.deleteNode(IP, port);
				pushDHT(IP, port);
				return result.replaceAll("\n", "  ");
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
		proxyServer.CCcommands(proxy);
	    proxyServer.initializeDataNode();


        int port = 9093;
        ServerSocket ss = new ServerSocket(port);
        System.out.println("Elastic DHT server running at " + String.valueOf(port));

//        System.out.println(proxy.addNode("192.168.0.219", 8001, 16568, 16578));
//        System.out.println(proxy.deleteNode("192.168.0.201", 8133));
//        System.out.println(proxy.addNode("192.168.0.229", 8003, 45875, 45885));
//        System.out.println(proxy.deleteNode("192.168.0.204", 8199));
//        System.out.println(proxy.addNode("192.168.0.215", 8004, 10460, 10470));
//        System.out.println(proxy.deleteNode("192.168.0.202", 8199));
//        System.out.println(proxy.deleteNode("192.168.0.210", 8177));
//        System.out.println(proxy.deleteNode("192.168.0.201", 8198));
//        System.out.println(proxy.deleteNode("192.168.0.207", 8161));
//        System.out.println(proxy.deleteNode("192.168.0.201", 8107));
//        System.out.println(proxy.addNode("192.168.0.221", 8005, 63555, 63565));
//        System.out.println(proxy.loadBalance("192.168.0.206", 8110, "192.168.0.205", 8111, 14));
//        System.out.println(proxy.loadBalance("192.168.0.205", 8146, "192.168.0.203", 8110, 11));
//        System.out.println(proxy.addNode("192.168.0.227", 8002, 58275, 58285));
//        System.out.println(proxy.addNode("192.168.0.220", 8001, 85506, 85516));
//        System.out.println(proxy.deleteNode("192.168.0.210", 8110));
//        System.out.println(proxy.deleteNode("192.168.0.209", 8146));
//        System.out.println(proxy.loadBalance("192.168.0.207", 8138, "192.168.0.201", 8168, 14));

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

class ProxyClient_Elastic{
    PrintWriter output;
    BufferedReader input;
    InputStream inputStream;
    OutputStream outputStream;
    
    Socket socket;
    ProxyServer proxy;
	public ProxyClient_Elastic(ProxyServer proxy) { 
		this.proxy = proxy;
//		this.address = address;
//		this.port = port;
//    	this.socket = s; 
//    	this.input = input;
//        this.output = output;
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
			System.out.println("SocketTimeoutException " + serverAddress + ":" + port + ". " + exception.getMessage());
			return false;
		} catch (IOException exception) {
//			socket.close();
//			System.out.println(
//					"IOException - Unable to connect to " + serverAddress + ":" + port + ". " + exception.getMessage());
			return false;
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
    
    public void processCommandDHTPush() throws Exception {
    	String timeStamp = new Date().toString();
    	System.out.println("Sending command" + " ---- " + timeStamp);
    	System.out.println();
        
    	Response response = new Response(true, this.proxy.getLookupTable().toJSON(), "Elastic DHT table");
        JsonObject params = null;
        JsonObject jobj = null;
		  params = Json.createObjectBuilder()
//		  .add("ip", ip)
//		  .add("port", port)
		  .add("result", response.toJSON())
		  .build();
		
		  jobj = Json.createObjectBuilder()
		  .add("method", "dhtpush")
		  .add("parameters", params)
		  .build();
    	
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

class RunDataNode_Elastic extends Thread {
	final String ip;
	final int port;
	public RunDataNode_Elastic(String ip, int port) {
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
			Process p = Runtime.getRuntime().exec("java -classpath .:../lib/* dht/elastic_DHT_centralized/DataNode " + ip + " " + dataPortNum);
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
