package dht.elastic_DHT_centralized;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import dht.common.Hashing;
import dht.common.response.Response;
import dht.server.Command;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.*;

public class ProxyServer extends Proxy {
    public static int REPLICATION_LEVEL = 3;
    public static int INITIAL_HASH_RANGE = 1000;
    public static int CURRENT_HASH_RANGE = 1000;
    public static int TOTAL_CCCOMMANDS = 100;
    public static int LOAD_PER_LOAD = 10;

    public ProxyServer(){
        super();
    }

    public static Proxy initializeEDHT(){
        try {
            // Read from the configuration file "config_ring.xml"
//              String xmlPath = System.getProperty("user.dir") + File.separator + "src" + File.separator + "dht" + File.separator + "elastic_DHT_centralized" + File.separator + "config_ElasticDHT.xml";
            String xmlPath = System.getProperty("user.dir") + File.separator + "dht" + File.separator + "elastic_DHT_centralized" + File.separator + "config_ElasticDHT.xml";

            File inputFile = new File(xmlPath);
            SAXReader reader = new SAXReader();
            Document document = reader.read(inputFile);

            // Read the elements in the configuration file
            Element rootElement = document.getRootElement();
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
    public void CCcommands(Proxy proxy) {
        BufferedWriter writer = null;
        String rootPath = System.getProperty("user.dir");
//        String path = rootPath + File.separator + "src" + File.separator + "dht" + File.separator + "elastic_DHT_centralized" + File.separator + "elastic_CCcommands.txt";
        String path = rootPath + File.separator + "dht" + File.separator + "elastic_DHT_centralized" + File.separator + "elastic_CCcommands.txt";

        try {
            writer = new BufferedWriter(new FileWriter(path), 32768);
            String[] availableCommands = {"add", "remove", "loadbalance"};
            String[] expand_shrink_commands = {"expand", "shrink"};
            String[] availableIPs = {"192.168.0.211","192.168.0.212","192.168.0.213","192.168.0.214",
                    "192.168.0.215","192.168.0.216","192.168.0.217","192.168.0.218","192.168.0.219","192.168.0.220",
                    "192.168.0.221", "192.168.0.222","192.168.0.223","192.168.0.224","192.168.0.225",
                    "192.168.0.226", "192.168.0.227","192.168.0.228","192.168.0.229","192.168.0.230"};
            String[] availablePorts = {"8001", "8002", "8003", "8004", "8005"};
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
				return proxy.loadBalance(fromIP, fromPort, toIP, toPort, numBuckets).replaceAll("\n", "  ");
			}
			else if (command.getAction().equals("add")) {
				String ip = command.getCommandSeries().get(0);
				int port = Integer.valueOf(command.getCommandSeries().get(1));
				int start = command.getCommandSeries().size() == 4 ? Integer.valueOf(command.getCommandSeries().get(2)) : -1;
				int end = command.getCommandSeries().size() == 4 ? Integer.valueOf(command.getCommandSeries().get(3)) : -1;
				
				String result = start == -1 && end == -1 ? proxy.addNode(ip, port) : proxy.addNode(ip, port, start, end);
				return result.replaceAll("\n", "  ");
			}
			else if (command.getAction().equals("remove")) {
				String IP = command.getCommandSeries().get(0);
				int port = Integer.valueOf(command.getCommandSeries().get(1));
				String result = proxy.deleteNode(IP, port);
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
