package dht.Ring;

import java.sql.Timestamp;
import java.util.*;

public class PhysicalNode {

    private String id;

    private String address;

    private int port;

    private String status;

    private LookupTable lookupTable;

    private List<VirtualNode> virtualNodes;

    public final static String STATUS_ACTIVE = "active";

    public final static String STATUS_INACTIVE = "inactive";

    public PhysicalNode() {
    }

    public PhysicalNode(String ID, String ip, int port, String status, List<VirtualNode> nodes){
        this.id = ID;
        this.address = ip;
        this.port = port;
        this.status = status;
        this.virtualNodes = nodes;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public LookupTable getLookupTable() {
        return lookupTable;
    }

    public void setLookupTable(LookupTable lookupTable) {
        this.lookupTable = lookupTable;
    }

    public List<VirtualNode> getVirtualNodes() {
        return virtualNodes;
    }

    public void setVirtualNodes(List<VirtualNode> virtualNodes) {
        this.virtualNodes = virtualNodes;
    }

    public void dataTransfer(VirtualNode fromNode, VirtualNode toNode, int start, int end){
        String address1 = lookupTable.getPhysicalNodeMap().get(fromNode.getPhysicalNodeId()).getAddress() + " (port: " +
                lookupTable.getPhysicalNodeMap().get(fromNode.getPhysicalNodeId()).getPort() + ")";
        String address2 = lookupTable.getPhysicalNodeMap().get(toNode.getPhysicalNodeId()).getAddress() + " (port: " +
                lookupTable.getPhysicalNodeMap().get(toNode.getPhysicalNodeId()).getPort() + ")";

        System.out.println("\nfrom virtual node " + fromNode.getHash() + " on " + address1 + "\n" +
                "to virutal node " + toNode.getHash() + " on " + address2 + ": ");
        System.out.println("\tTranfering data for hash range of (" + start +", " + end + ")");
    }

    public void addNode(String ip, int port, int hash){
        // Create an id for the new physical node
        String physicalNodeID =  "D" + ip.substring(ip.length() - 3)+ "-" + Integer.toString(port);
        // Create a new virtual node that maps to this physical node
        // Assume just 1 virtual node maps to this physical node
        VirtualNode vNode = new VirtualNode(hash, physicalNodeID);
        // Put the virtual node on the ring
        if (lookupTable.getTable().add(vNode) == false){
            System.out.println("This virtual node already exists!");
            return;
        }
        // Get the index of the inserted virtual node in the BinarySearchList
        int index = vNode.getIndex();
        VirtualNode next1 = lookupTable.getTable().next(index);
        VirtualNode next2 = lookupTable.getTable().next(index+1);
        VirtualNode next3 = lookupTable.getTable().next(index+2);
        VirtualNode pre1 = lookupTable.getTable().pre(index);
        VirtualNode pre2 = lookupTable.getTable().pre(index-1);
        VirtualNode pre3 = lookupTable.getTable().pre(index-2);
        // Check if this physical node already exits in the physicalNodeMap
        if (!lookupTable.getPhysicalNodeMap().containsKey(physicalNodeID)){
            List<VirtualNode> list = new ArrayList<>();
            list.add(vNode);
            PhysicalNode physicalNode = new PhysicalNode(physicalNodeID, ip, port, STATUS_ACTIVE, list);
            lookupTable.getPhysicalNodeMap().put(physicalNodeID, physicalNode);
        }
        else{
            lookupTable.getPhysicalNodeMap().get(physicalNodeID).getVirtualNodes().add(vNode);
        }

        System.out.println("Adding node at " + hash + ":");
        dataTransfer(next1, vNode, pre3.getHash()+1, pre2.getHash());
        dataTransfer(next2, vNode, pre2.getHash()+1, pre1.getHash());
        dataTransfer(next3, vNode, pre1.getHash()+1, hash);

        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        lookupTable.setEpoch(timestamp.getTime());
    }


    // Delete virtual node by its hash value
    public void deleteNode(int hash) {
        VirtualNode vNode = new VirtualNode(hash);
        int index = Collections.binarySearch(lookupTable.getTable(), vNode);
        if (index < 0){
            System.out.println("hash " + hash + " is not a virtual node.");
            return;
        }
        VirtualNode next1 = lookupTable.getTable().next(index);
        VirtualNode next2 = lookupTable.getTable().next(index+1);
        VirtualNode next3 = lookupTable.getTable().next(index+2);
        VirtualNode pre1 = lookupTable.getTable().pre(index);
        VirtualNode pre2 = lookupTable.getTable().pre(index-1);
        VirtualNode pre3 = lookupTable.getTable().pre(index-2);

        // Delete the virtual node from the ring of virtual nodes
        VirtualNode virtualNodeToDelete = lookupTable.getTable().remove(index);

        System.out.println("\nDeleting node at " + hash + ":");
        dataTransfer(pre2, next1, pre3.getHash()+1, pre2.getHash());
        dataTransfer(pre1, next2, pre2.getHash()+1, pre1.getHash());
        dataTransfer(next1, next3, pre1.getHash()+1, hash);

        // Remove the virtual node from its physcial node's virtual node list
        List<VirtualNode> list = lookupTable.getPhysicalNodeMap().get(virtualNodeToDelete.getPhysicalNodeId()).getVirtualNodes();
        int idx = Collections.binarySearch(list, virtualNodeToDelete);
        lookupTable.getPhysicalNodeMap().get(virtualNodeToDelete.getPhysicalNodeId()).getVirtualNodes().remove(idx);

        // Update the local timestamp
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        lookupTable.setEpoch(timestamp.getTime());
    }

    //// Delete virtual node by its hash value
    public void deleteNode(VirtualNode node){
        int index = Collections.binarySearch(lookupTable.getTable(), node);
        if (index < 0){
            return;
        }

        VirtualNode next1 = lookupTable.getTable().next(index);
        VirtualNode next2 = lookupTable.getTable().next(index+1);
        VirtualNode next3 = lookupTable.getTable().next(index+2);
        VirtualNode pre1 = lookupTable.getTable().pre(index);
        VirtualNode pre2 = lookupTable.getTable().pre(index-1);
        VirtualNode pre3 = lookupTable.getTable().pre(index-2);

        // Delete the virtual node from the ring of virtual nodes
        VirtualNode virtualNodeToDelete = lookupTable.getTable().remove(index);

        System.out.println("\nDeleting node at " + node.getHash() + ":");
        dataTransfer(pre2, next1, pre3.getHash()+1, pre2.getHash());
        dataTransfer(pre1, next2, pre2.getHash()+1, pre1.getHash());
        dataTransfer(next1, next3, pre1.getHash()+1, node.getHash());

        // Update the local timestamp
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        lookupTable.setEpoch(timestamp.getTime());
    }

    public void failNode(String ip, int port){
        String physicalNodeID =  "D" + ip.substring(ip.length() - 3)+ "-" + Integer.toString(port);
        lookupTable.getPhysicalNodeMap().get(physicalNodeID).setStatus(STATUS_INACTIVE);
        List<VirtualNode> virtualNodes = lookupTable.getPhysicalNodeMap().get(physicalNodeID).getVirtualNodes();
        if (virtualNodes.size() == 0){
            return;
        }
        for (VirtualNode node : virtualNodes ){
            deleteNode(node);
        }
        // Set the virtual node list of the failed node to be empty
        lookupTable.getPhysicalNodeMap().get(physicalNodeID).setVirtualNodes(new ArrayList<VirtualNode>());
    }

    // Change the position of a virtual node on the ring to balance load
    public void loadBalance(int delta, VirtualNode node){ // move the node clockwise if delta > 0, counterclockwise if delta < 0
        int oldHash = node.getHash();
        int newHash = oldHash + delta;
        int index = Collections.binarySearch(lookupTable.getTable(), node);
        if (index < 0){
            System.out.println("hash " + oldHash + " is not a virtual node.");
            return;
        }
        node.setHash(newHash);
        // Get the 3rd successor of the virtual node
        VirtualNode thirdSuccessor = lookupTable.getTable().next(index+2);
        if (delta > 0) {
            dataTransfer(thirdSuccessor, node, oldHash + 1, newHash);
        }
        else if (delta < 0){
            dataTransfer(node, thirdSuccessor,newHash + 1, oldHash);
        }
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        lookupTable.setEpoch(timestamp.getTime());
    }

    public int hashFunction(String s){
        // Set it to 64 here just for test purpose. It will be defined in the configuration file.
        int maxHash = 64;
        int hash = String.valueOf(s).hashCode() % maxHash;
        return hash;
    }
    public void writeRequest(String key){
        int hash = hashFunction(key);
        VirtualNode hashValue = new VirtualNode(hashFunction(key));
        VirtualNode vNode = lookupTable.getTable().find(hashValue);
        // Store replica in two successors
        VirtualNode replica_1 = lookupTable.getTable().next(vNode);
        VirtualNode replica_2 = lookupTable.getTable().next(replica_1);
        write(vNode, hash);
        write(replica_1, hash);
        write(replica_2, hash);
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        lookupTable.setEpoch(timestamp.getTime());

    }

    // A helper function for writeRequest() method
    public void write(VirtualNode virtualNode, int hash){
        String address = lookupTable.getPhysicalNodeMap().get(virtualNode.getPhysicalNodeId()).getAddress() + " (port: " +
                lookupTable.getPhysicalNodeMap().get(virtualNode.getPhysicalNodeId()).getPort() + ")";
        System.out.println("\nConnecting to " + address + " to write on virtual node " + virtualNode.getHash() +
                " for hash value " + hash);
        System.out.println("Writing completed");
    }

    public void readRequest(String key){
        int hash = hashFunction(key);
        VirtualNode hashValue = new VirtualNode();
        VirtualNode vNode = lookupTable.getTable().find(hashValue);
        VirtualNode replica_1 = lookupTable.getTable().next(vNode);
        VirtualNode replica_2 = lookupTable.getTable().next(replica_1);
        read(vNode, hash);
        read(replica_1, hash);
        read(replica_2, hash);
    }
    public void read(VirtualNode virtualNode, int hash){
        String address = lookupTable.getPhysicalNodeMap().get(virtualNode.getPhysicalNodeId()).getAddress() + " (port: " +
                lookupTable.getPhysicalNodeMap().get(virtualNode.getPhysicalNodeId()).getPort() + ")";
        System.out.println("\nConnecting to " + address + " to read for hash value " + hash);
        System.out.println("Reading completed");

    }
}
