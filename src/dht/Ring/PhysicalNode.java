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
        Indexable next1 = lookupTable.getTable().next(index);
        Indexable next2 = lookupTable.getTable().next(index+1);
        Indexable next3 = lookupTable.getTable().next(index+2);
        Indexable pre1 = lookupTable.getTable().pre(index);
        Indexable pre2 = lookupTable.getTable().pre(index-1);
        Indexable pre3 = lookupTable.getTable().pre(index-2);
        // Check if this physical node already exits in the physicalNodeMap
        if (!lookupTable.getPhysicalNodeMap().containsKey(physicalNodeID)){
            List<VirtualNode> list = new LinkedList<>();
            list.add(vNode);
            PhysicalNode physicalNode = new PhysicalNode(physicalNodeID, ip, port, STATUS_ACTIVE, list);
            lookupTable.getPhysicalNodeMap().put(physicalNodeID, physicalNode);
        }
        else{
            lookupTable.getPhysicalNodeMap().get(physicalNodeID).getVirtualNodes().add(vNode);
        }

        dataTransfer(next1, vNode, pre3.getHash()+1, pre2.getHash());
        dataTransfer(next2, vNode, pre2.getHash()+1, pre1.getHash());
        dataTransfer(next3, vNode, pre1.getHash()+1, hash);

        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        lookupTable.setEpoch(timestamp.getTime());
    }

    public void dataTransfer(Indexable fromNode, Indexable toNode, int start, int end){
        String address1 = lookupTable.getPhysicalNodeMap().get(fromNode.getPhysicalNodeId()).getAddress() + " (port: " +
                lookupTable.getPhysicalNodeMap().get(fromNode.getPhysicalNodeId()).getPort() + ")";
        String address2 = lookupTable.getPhysicalNodeMap().get(toNode.getPhysicalNodeId()).getAddress() +
                lookupTable.getPhysicalNodeMap().get(toNode.getPhysicalNodeId()).getPort();

        System.out.println("from virtual node " + fromNode.getHash() + " on " + address1 + "\n" +
                "to virutal node" + toNode.getHash() + " on " + address2 + ": ");
        System.out.println("\tTranfering data for hash range of (" + start +", " + end + ")");
    }

    public void deleteNode(int hash) {
        Indexable vNode = new VirtualNode(hash);
        int index = Collections.binarySearch(lookupTable.getTable(), vNode);
        if (index < 0){
            System.out.println("hash " + hash + " is not a virtual node.");
            return;
        }

        Indexable next1 = lookupTable.getTable().next(index);
        Indexable next2 = lookupTable.getTable().next(index+1);
        Indexable next3 = lookupTable.getTable().next(index+2);
        Indexable pre1 = lookupTable.getTable().pre(index);
        Indexable pre2 = lookupTable.getTable().pre(index-1);
        Indexable pre3 = lookupTable.getTable().pre(index-2);

        // Delete the virtual node from the ring of virtual nodes
        Indexable virtualNodeToDelete = lookupTable.getTable().remove(index);

        dataTransfer(virtualNodeToDelete, next1, pre3.getHash()+1, pre2.getHash());
        dataTransfer(virtualNodeToDelete, next2, pre2.getHash()+1, pre1.getHash());
        dataTransfer(virtualNodeToDelete, next3, pre1.getHash()+1, hash);

        // Remove the virtual node from its physcial node's virtual node list
        List<VirtualNode> list = lookupTable.getPhysicalNodeMap().get(virtualNodeToDelete.getPhysicalNodeId()).getVirtualNodes();
        int idx = Collections.binarySearch(list, virtualNodeToDelete);
        lookupTable.getPhysicalNodeMap().get(virtualNodeToDelete.getPhysicalNodeId()).getVirtualNodes().remove(idx);

        // Update the local timestamp
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        lookupTable.setEpoch(timestamp.getTime());
    }

}
