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
        }
        // Get the index of the inserted virtual node in the BinarySearchList
        int index = vNode.getIndex();
        Indexable next1 = lookupTable.getTable().next(index);
        Indexable next2 = lookupTable.getTable().next(index+1);
        Indexable next3 = lookupTable.getTable().next(index+2);
        Indexable pre1 = lookupTable.getTable().next(index);
        Indexable pre2 = lookupTable.getTable().next(index-1);
        Indexable pre3 = lookupTable.getTable().next(index-2);
        List<VirtualNode> list = new LinkedList<>();
        list.add(vNode);
        PhysicalNode physicalNode = new PhysicalNode(physicalNodeID, ip, port, STATUS_ACTIVE, list);
        dataTransfer(next1, vNode, pre3.getHash()+1, pre2.getHash());
        dataTransfer(next2, vNode, pre2.getHash()+1, pre1.getHash());
        dataTransfer(next3, vNode, pre1.getHash()+1, hash);

        lookupTable.getPhysicalNodeMap().put(physicalNodeID, physicalNode);
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




    }

}
