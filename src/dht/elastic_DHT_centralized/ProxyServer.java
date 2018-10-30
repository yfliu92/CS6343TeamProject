package dht.elastic_DHT_centralized;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

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
        System.out.println(proxy.addNode("192.168.0.211", 8100, 900, 910));
//        System.out.println(proxy.deleteNode("192.168.0.201", 8100));
//        System.out.println(proxy.loadBalance("192.168.0.204", 8100, "192.168.0.210", 8100, 12));

    	System.out.println("Elastic DHT server running at 9093");
        ServerSocket listener = new ServerSocket(9093);;

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
