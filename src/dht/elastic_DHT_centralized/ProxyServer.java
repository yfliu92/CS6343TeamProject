package dht.elastic_DHT_centralized;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import dht.server.Command;

import java.io.*;
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

            // Get the port
            Element port = document.getRootElement().element("port");
            int startPort = Integer.parseInt(port.element("startPort").getStringValue());
            int portRange = Integer.parseInt(port.element("portRange").getStringValue());

            // Get the IPs
            Element nodes = document.getRootElement().element("nodes");
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
            int loadPerNode = HashAndReplicationConfig.CURRENT_HASH_RANGE / physicalNodes.size();
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
    public void CCcommands(Proxy proxy) {
        BufferedWriter writer = null;

        try {
            writer = new BufferedWriter(new FileWriter("elastic_CCcommands.txt"), 32768);
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
            int total_commands = HashAndReplicationConfig.TOTAL_CCCOMMANDS;
            for (int i = 0; i < total_commands; i++){
                Random ran = new Random();
                // Randomly pick a command between "expand" and "shrink" when i is 99, 199, 299....
                if (i % 100 == 99){
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
                    int ran_start = ran.nextInt(HashAndReplicationConfig.INITIAL_HASH_RANGE);
                    //int loadPerNode = (HashAndReplicationConfig.CURRENT_HASH_RANGE / currentPNodes.size()) * HashAndReplicationConfig.REPLICATION_LEVEL;
                    int ran_end = (ran_start + HashAndReplicationConfig.loadPerNode) % HashAndReplicationConfig.INITIAL_HASH_RANGE;
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

                    int numOfBuckets = ran.nextInt(50) + 50;
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
			if (command.getAction().equals("find")) {
				return getFindInfo(command.getInput());
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
				return proxy.listNodes();
			}
			else {
				return "Command not supported";
			}
		}
		catch (Exception ee) {
			return "Illegal command";
		}

	}
    
    public static void main(String[] args) throws IOException {
        ProxyServer proxyServer = new ProxyServer();
        //Initialize the Elastic DHT cluster
        Proxy proxy = initializeEDHT();
        proxyServer.CCcommands(proxy);
//        System.out.println(proxy.addNode("192.168.0.217", 8004, 473, 773));
//        System.out.println(proxy.deleteNode("192.168.0.201", 8100));
//        System.out.println(proxy.loadBalance("192.168.0.204", 8100, "192.168.0.210", 8100, 12));

        int port = 9093;
    	System.out.println("Elastic DHT server running at " + String.valueOf(port));
        ServerSocket listener = new ServerSocket(port);

        try {
            while (true) {
            	Socket socket = listener.accept();
            	System.out.println("Connection accepted" + " ---- " + new Date().toString());
                try {
                	BufferedReader in = new BufferedReader(
                            new InputStreamReader(socket.getInputStream()));
                    PrintWriter out =
                            new PrintWriter(socket.getOutputStream(), true);
                	String msg;
                	while(true) {
                		try {
                    		msg = in.readLine();
                        	if (msg != null) {
                            	System.out.println("Request received: " + msg + " ---- " + new Date().toString());

                                String response = proxyServer.getResponse(msg, proxy);
                                out.println(response);
                                System.out.println("Response sent: " + response);
                        	}
                        	else {
                        		System.out.println("Connection end " + " ---- " + new Date().toString());
                        		break;
                        	}
                		}
                		catch (Exception ee) {
                    		System.out.println("Connection reset " + " ---- " + new Date().toString());
                    		break;
                		}

                	}

                } finally {
                    socket.close();
                }
            }
        }
        finally {
            listener.close();
        }
        
    }
}
