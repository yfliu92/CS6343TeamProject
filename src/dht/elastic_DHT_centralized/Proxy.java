package dht.elastic_DHT_centralized;


import java.sql.Timestamp;
import java.util.*;

public class Proxy{
    private String id = "PROXY";

    private String ip;

    private int port;

    private LookupTable lookupTable;

    public Proxy(){
    }
    public Proxy (String ip, int port){
        this.ip = ip;
        this.port = port;
    }


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

    public String dataTransfer(String fromID, String toID, int hash){
        return "\nFrom " + fromID + " to " + toID + ": transferring data for bucket " + Integer.toString(hash);
    }

    // Add a physical node without specifying for what range of buckets
    public String addNode(String ip, int port){
        // ID of the new node will be automatically created by the PhysicalNode constructor
        PhysicalNode newNode = new PhysicalNode(ip, port, "active");
        String id = newNode.getId();
        lookupTable.getPhysicalNodesMap().put(id, newNode);
        int loadPerNode = (lookupTable.getBucketsTable().size() / lookupTable.getPhysicalNodesMap().size());
        // Use this new node as a replica for the first loadPerNode buckets in the bucketsTable
        String result = "";
        for (int i = 0; i < loadPerNode; i++){
            HashMap<String, String> replicas = lookupTable.getBucketsTable().get(i);
            String fromID = replicas.entrySet().iterator().next().getKey();
            replicas.put(id, id);
            result += dataTransfer(fromID, id, i);
        }

        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        lookupTable.setEpoch(timestamp.getTime());
        // Proxy updates the lookupTable of all physical nodes
        for (PhysicalNode pNode: lookupTable.getPhysicalNodesMap().values()){
            pNode.setLookupTable(lookupTable);
        }

        return "true|node " + id + " is added successfully.\n" + result;
    }

    // Add a physical node, clearly specify for what range of buckets it will serve as a replica
    public String addNode(String ip, int port, int start, int end){
        PhysicalNode newNode = new PhysicalNode(ip, port, "active");
        String id = newNode.getId();
        lookupTable.getPhysicalNodesMap().put(id, newNode);
        // Use this new node as a replica for the first loadPerNode buckets in the bucketsTable
        String result = "";
        for (int i = start; i < end; i++){
            HashMap<String, String> replicas = lookupTable.getBucketsTable().get(i);
            String fromID = replicas.entrySet().iterator().next().getKey();
            replicas.put(id, id);
            result += dataTransfer(fromID, id, i);
        }

        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        lookupTable.setEpoch(timestamp.getTime());
        // Proxy updates the lookupTable of all physical nodes
        for (PhysicalNode pNode: lookupTable.getPhysicalNodesMap().values()){
            pNode.setLookupTable(lookupTable);
        }

        return "true|node " + id + " is added successfully.\n" + result;
    }
    public String deleteNode(String ip, int port) {
        String nodeID = ip + "-" + Integer.toString(port);

        // Try to remove this physical node from the physicalNodesMap
        // The node doesn't exit if .remove() returns null
        PhysicalNode node = lookupTable.getPhysicalNodesMap().remove(nodeID);
        if (node == null){
            return "false|This node doesn't exist!";
        }
        node.setStatus("inactive");
        // Get the bucketsTable
        HashMap<Integer, HashMap<String, String>> table = lookupTable.getBucketsTable();
        String result = "";
        for (int i = 0; i < table.size(); i++) {
            if (table.get(i).get(nodeID) == null)
                continue;
            else {
                // Remove this node ID from the bucket's replicas
                table.get(i).remove(nodeID);
                // Since the node is deleted/failed, must find another existing replica to transfer data to the newly added replica
                String fromID = table.get(i).entrySet().iterator().next().getKey();

                // Get a list of all physical node ids
                Set<String> idSet = lookupTable.getPhysicalNodesMap().keySet();
                List<String> idList = new ArrayList<>(idSet);
                String toID;
                String valueReturned;
                // Randomly select a node to replace the deleted node
                do {
                    Random ran = new Random();
                    toID = idList.get(ran.nextInt(idList.size()));
                    valueReturned = table.get(i).put(toID, toID);
                } while (valueReturned != null);

                result += (dataTransfer(fromID, toID, i));
            }
        }
        // Update the epoch time
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        lookupTable.setEpoch(timestamp.getTime());
        // Proxy updates the lookupTable of all physical nodes
        for (PhysicalNode pNode: lookupTable.getPhysicalNodesMap().values()){
            pNode.setLookupTable(lookupTable);
        }
        return "true|the node of " + nodeID + " has been successfully removed\n" + result;
    }
    public String loadBalance(String fromIP, int fromPort, String toIP, int toPort, int numOfBuckets){
        String fromID = fromIP + "-" + Integer.toString(fromPort);
        String toID = toIP + "-" + Integer.toString(toPort);
        // Check if the Physical node of fromID exists or not
        PhysicalNode fromNode = lookupTable.getPhysicalNodesMap().get(fromID);
        if (fromNode == null){
            return "false|This node of " + fromID + "doesn't exist!";
        }
        PhysicalNode toNode = lookupTable.getPhysicalNodesMap().get(fromID);
        if (toNode == null){
            // Create a new physical node of toID
            PhysicalNode newNode = new PhysicalNode(toIP, toPort, "active");
            lookupTable.getPhysicalNodesMap().put(toID, newNode);
        }
        // Get the bucketsTable
        HashMap<Integer, HashMap<String, String>> table = lookupTable.getBucketsTable();
        int count = numOfBuckets;
        int i = 0;
        String result = "";
        while (count > 0){
            if (table.get(i).get(fromID) == null)
                i++;
            else {
                table.get(i).remove(fromID);
                table.get(i).put(toID, toID);
                result += dataTransfer(fromID, toID, i) + " ";
                i++;
                count--;
                }
        }
        // Update the epoch number
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        lookupTable.setEpoch(timestamp.getTime());
        // Proxy updates the lookupTable of all physical nodes
        for (PhysicalNode pNode: lookupTable.getPhysicalNodesMap().values()){
            pNode.setLookupTable(lookupTable);
        }
        return "true|loadBalance from " + fromID + " to " + toID + " for " + numOfBuckets + " buckets: " + result;
    }

}
