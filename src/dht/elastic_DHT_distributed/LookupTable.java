package dht.elastic_DHT_distributed;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.sql.Timestamp;
import java.util.*;

public class LookupTable {
    private long epoch;
    // Integer represents the index of the hash bucket
    // HashMap<String, String> stores the replicas for that hash bucket
    private HashMap<Integer, HashMap<String, String>> bucketsTable;
    private HashMap<String, PhysicalNode> physicalNodesMap;
//    private static volatile LookupTable instance = null;
    public LookupTable(){
    };
//    private LookupTable() {
//        initialize();
//    }

//    public static LookupTable getInstance() {
//        if (instance == null) {
//            synchronized(LookupTable.class) {
//                if (instance == null){
//                    instance = new LookupTable();
//                }
//            }
//        }
//        return instance;
//    }
//
//    public void initialize(){
//        try {
//            // Read from the configuration file "config_ring.xml"
//            String xmlPath = System.getProperty("user.dir") + File.separator + "src" + File.separator + "dht" + File.separator + "elastic_DHT_distributed" + File.separator + "config_ElasticDHT.xml";
//            System.out.println(xmlPath);
//            File inputFile = new File(xmlPath);
//            SAXReader reader = new SAXReader();
//            Document document = reader.read(inputFile);
//
//            // Read the elements in the configuration file
//            int numOfReplicas = Integer.parseInt(document.getRootElement().element("replicationLevel").getStringValue());
//            int hashRange = Integer.parseInt(document.getRootElement().element("hashRange").getStringValue());
//
//            Element nodes = document.getRootElement().element("nodes");
//            List<Element> listOfNodes = nodes.elements();
//            int numOfNodes = listOfNodes.size();
//            HashMap<Integer, HashMap<String, String>> table = new HashMap<>();
//            HashMap<String, PhysicalNode> physicalNodes = new HashMap<>();
//
//            for (int i = 0; i < numOfNodes; i++){
//                String ip = listOfNodes.get(i).element("ip").getStringValue();
//                int port = Integer.parseInt(listOfNodes.get(i).element("port").getStringValue());
//                String nodeID = ip + "-" + Integer.toString(port);
//                PhysicalNode node = new PhysicalNode(ip, port, "active");
//                physicalNodes.put(nodeID, node);
//            }
//            // During initialization, hashRange is evenly distributed among the physical nodes
//            // If hashRange is 1000 and there are 10 physical nodes in total
//            // then the first node gets assigned (0, 99)
//            // The second node gets assigned (100, 199)
//            // ...
//            // The last node gets assigned (900, 999)
//            int loadPerNode = hashRange / physicalNodes.size();
//            // Define the start hash value for hash nodes
//            int start = 0;
//            // Get a list of all physical node ids
//            Set<String> idSet = physicalNodes.keySet();
//            List<String> idList = new ArrayList<>(idSet);
//            Collections.sort(idList);
//            int numOfPhysicalNodes = idList.size();
//
//            for (int i = 0; i < numOfPhysicalNodes; i++){
//                for (int j = start; j < start + loadPerNode; j++){
//                    HashMap<String, String> replicas = new HashMap<>();
//                    for (int k = 0; k < numOfReplicas; k++) {
//                        replicas.put(idList.get((i + k) % numOfPhysicalNodes), idList.get((i + k) % numOfPhysicalNodes));
//                    }
//                    table.put(j, replicas);
//
//                }
//                start += loadPerNode;
//
//            }
//            // Create a lookupTable and set it to every physical node
//            LookupTable t = new LookupTable();
//            t.setBucketsTable(table);
//            t.setPhysicalNodesMap(physicalNodes);
//            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
//            t.setEpoch(timestamp.getTime());
//
//            for (PhysicalNode node : physicalNodes.values()){
//                node.setLookupTable(t);
//            }
//
////            System.out.print("After initialization, the bucketsTable looks as follows: \n");
////            for(int i = 0; i < hashRange; i++) {
////                System.out.print("\nfor hash bucket " + i + ": ");
////                for (String id : table.get(i).keySet()){
////                    System.out.print(id + ", ");
////                }
////
////            }
////            System.out.print("\n");
//
//
//        }catch(DocumentException e) {
//            e.printStackTrace();
//        }
//
//    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public HashMap<Integer, HashMap<String, String>> getBucketsTable() {
        return bucketsTable;
    }

    public void setBucketsTable(HashMap<Integer, HashMap<String, String>> bucketsTable) {
        this.bucketsTable = bucketsTable;
    }

    public HashMap<String, PhysicalNode> getPhysicalNodesMap() {
        return physicalNodesMap;
    }

    public void setPhysicalNodesMap(HashMap<String, PhysicalNode> physicalNodesMap) {
        this.physicalNodesMap = physicalNodesMap;
    }
}

