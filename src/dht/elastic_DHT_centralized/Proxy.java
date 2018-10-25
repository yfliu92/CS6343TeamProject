package dht.elastic_DHT_centralized;


import java.sql.Timestamp;
import java.util.*;

public class Proxy{
    private String id = "PROXY";

    private String ip;

    private int port;

    private LookupTable lookupTable;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public LookupTable getLookupTable() {
        return lookupTable;
    }

    public void setLookupTable(LookupTable lookupTable) {
        this.lookupTable = lookupTable;
    }

    // Add a physical node without specifying for what range of buckets
    public String addNode(String ip, int port){
        // ID of the new node will be automatically created by the PhysicalNode constructor
        PhysicalNode newNode = new PhysicalNode(ip, port, "active");
        String id = newNode.getId();
        lookupTable.getPhysicalNodesMap().put(id, newNode);
        int loadPerNode = (lookupTable.getBucketsTable().size() / lookupTable.getPhysicalNodesMap().size()) * Config.NUM_OF_REPLICAS;
        // Use this new node as a replica for the first loadPerNode buckets in the bucketsTable
        for (int i = 0; i < loadPerNode; i++){
            lookupTable.getBucketsTable().get(i).put(id, id);
        }

        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        lookupTable.setEpoch(timestamp.getTime());

        return "true|node " + id + "is added successfully.";
    }

    // Add a physical node, clearly specify for what range of buckets it will serve as a replica
    public String addNode(String ip, int port, int start, int end){
        PhysicalNode newNode = new PhysicalNode(ip, port, "active");
        String id = newNode.getId();
        lookupTable.getPhysicalNodesMap().put(id, newNode);
        // Use this new node as a replica for the first loadPerNode buckets in the bucketsTable
        for (int i = start; i < end; i++){
            lookupTable.getBucketsTable().get(i).put(id, id);
        }

        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        lookupTable.setEpoch(timestamp.getTime());

        return "true|node " + id + "is added successfully.";
    }
    public String deleteNode(String ip, int port) {
        String nodeID = ip + Integer.toString(port);

        // Try to remove this physical node from the physicalNodesMap
        // The node doesn't exit if .remove() returns null
        PhysicalNode node = lookupTable.getPhysicalNodesMap().remove(nodeID);
        if (node == null){
            return "false|This node doesn't exist!";
        }
        node.setStatus("inactive");
        // Get the bucketsTable
        ArrayList<HashMap<String, String>> table = lookupTable.getBucketsTable();
        for (int i = 0; i < table.size(); i++) {
            if (table.get(i).get(nodeID) == null)
                continue;
            else {
                table.get(i).remove(nodeID);
                // Get a list of all physical node ids
                Set<String> idSet = lookupTable.getPhysicalNodesMap().keySet();
                List<String> idList = new ArrayList<>(idSet);
                // Randomly select a node to replace the deleted node
                String result;
                do {
                    Random ran = new Random();
                    String id = idList.get(ran.nextInt(idList.size()));
                    result = table.get(i).put(id, id);
                } while (result != null);
            }
        }
        return "true|the node of " + nodeID + "has been successfully removed";
    }
    public String loadBalance(String fromIP, int fromPort, String toIP, int toPort, int numOfBuckets){
        String fromID = fromIP + Integer.toString(fromPort);
        String toID = toIP + Integer.toString(toPort);
        // Check if the Physical node of fromID exists or not
        PhysicalNode fromNode = lookupTable.getPhysicalNodesMap().get(fromID);
        if (fromNode == null){
            return "false|This node of " + fromID + "doesn't exist!";
        }
        PhysicalNode toNode = lookupTable.getPhysicalNodesMap().get(fromID);
        if (fromNode == null){
            // Create a new physical node of toID
            PhysicalNode newNode = new PhysicalNode(toIP, toPort, "active");
            lookupTable.getPhysicalNodesMap().put(toID, newNode);
        }
        // Get the bucketsTable
        ArrayList<HashMap<String, String>> table = lookupTable.getBucketsTable();
        int count = numOfBuckets;
        int i = 0;
        while (count > 0){
            if (table.get(i).get(fromID) == null)
                i++;
            else {
                table.get(i).remove(fromID);
                table.get(i).put(toID, toID);
                i++;
                count--;
                }
        }
        return "true|loadBalance from " + fromID + " to " + toID + " for " + numOfBuckets + " buckets has been completed.";
    }

}
