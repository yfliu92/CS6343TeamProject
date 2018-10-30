package dht.elastic_DHT_distributed;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;

public class ProxyServer extends PhysicalNode {
    public ProxyServer(){
        super();
    }

    public static void initializeEDHT(){
        try {
            // Read from the configuration file "config_ring.xml"
            String xmlPath = System.getProperty("user.dir") + File.separator + "dht" + File.separator + "elastic_DHT_distributed" + File.separator + "config_ElasticDHT.xml";
            File inputFile = new File(xmlPath);
            SAXReader reader = new SAXReader();
            Document document = reader.read(inputFile);

            // Read the elements in the configuration file
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


        }catch(DocumentException e) {
            e.printStackTrace();
        }

    }
    public static void main(String[] args) throws IOException {
        ProxyServer proxyServer = new ProxyServer();
        //Initialize the Elastic DHT cluster
        initializeEDHT();


    }
}
