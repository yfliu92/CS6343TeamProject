package dht.Ring;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import dht.common.Hashing;
import dht.server.Command;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import storage_server.Datum;

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
            // If hashRange is 1000 and there are 10 physical nodes in total, then stepSize is 100
            // The first physical node will start from 0 and map to virtual nodes of hash 0, 100, 200,...,900
            // The second physical node will start from 10 and map to virtual nodes of hash 10, 110, 210,...,910
            // ...
            // The last physical node will start from 90 and map to virtual nodes of hash 90, 190, 290,...,990
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

//            String result = "After initialization, virtual nodes include: \n";
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

	public static String getFindInfo(String input) {
		return input.toUpperCase();
	}

	public String getResponse(String commandStr, DataStore dataStore) {
		Command command = new Command(commandStr);
		try {
			if (command.getAction().equals("find")) {
				return getFindInfo(command.getInput());
			}
			else if (command.getAction().equals("read")) {
				String dataStr = command.getCommandSeries().get(0);
				return dataStore.readRes(dataStr);
			}
			else if (command.getAction().equals("write")) {
				String dataStr = command.getCommandSeries().get(0);
				int rawhash = Hashing.getHashValFromKeyword(dataStr);
				int virtualnode = command.getCommandSeries().size() > 1 ? Integer.parseInt(command.getCommandSeries().get(1)) : super.getVirtualNode(dataStr).getHash();
				return dataStore.writeRes(dataStr, rawhash, new int[]{virtualnode});
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
//				return "remove";
			}
			else if (command.getAction().equals("info")) {
				return super.listNodes();
			}
			else {
				return "Command not supported";
			}
		}
		catch (Exception ee) {
			return "Illegal command " + ee.toString();
		}
	}
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		ProxyServer proxy = new ProxyServer();
		//Initialize the ring cluster
		proxy.initializeRing();
		
		DataStore dataStore = new DataStore();
		
		int port = 9091;
    	System.out.println("Ring server running at " + String.valueOf(port));
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
                            	System.out.println();

                                String response = proxy.getResponse(msg, dataStore);
                                out.println(response);
                                System.out.println("Response sent: " + response);
                                System.out.println();
                        	}
                        	else {
                        		System.out.println("Connection end " + " ---- " + new Date().toString());
                        		System.out.println();
                        		break;
                        	}
                		}
                		catch (Exception ee) {
                    		System.out.println("Connection reset " + " ---- " + new Date().toString());
                    		System.out.println();
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
