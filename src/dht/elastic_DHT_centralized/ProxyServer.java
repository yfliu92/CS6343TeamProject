package dht.elastic_DHT_centralized;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;

public class ProxyServer extends Proxy {
    public ProxyServer(){
        super();
    }

    public static Proxy initializeEDHT(){
        try {
            // Read from the configuration file "config_ring.xml"
            String xmlPath = System.getProperty("user.dir") + File.separator + "src" + File.separator + "dht" + File.separator + "elastic_DHT_centralized" + File.separator + "config_ElasticDHT.xml";
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
            return proxy;

        }catch(DocumentException e) {
            e.printStackTrace();
            return null;
        }

    }
    public static void main(String[] args) throws IOException {
        ProxyServer proxyServer = new ProxyServer();
        //Initialize the Elastic DHT cluster
        Proxy proxy = initializeEDHT();
        // Call addNode() in the form of proxy.addNode()
        // Call deleteNode() in the form of proxy.addNode()
        // Call loadBalance() in the form of proxy.loadBalance()
        // For local test use
        System.out.println(proxy.addNode("192.168.0.211", 8100, 900, 910));
//        System.out.println(proxy.deleteNode("192.168.0.201", 8100));
//        System.out.println(proxy.loadBalance("192.168.0.204", 8100, "192.168.0.210", 8100, 12));


    }
}
