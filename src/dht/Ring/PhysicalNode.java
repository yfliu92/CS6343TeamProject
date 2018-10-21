package dht.Ring;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

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

    public void addNode(String ip, int port){
        String newNodeID =  "D" + ip.substring(ip.length() - 3);
        List<VirtualNode> tempList = new LinkedList<>();
        HashMap<String, PhysicalNode> physicalNodes = lookupTable.getPhysicalNodeMap();
        for (PhysicalNode node : physicalNodes.values()){
            for (int i = 0; i < node.getVirtualNodes().size() / physicalNodes.size(); i++){
                VirtualNode temp = node.getVirtualNodes().remove(node.getVirtualNodes().size() - 1);
                temp.setPhysicalNodeId(newNodeID);
                tempList.add(temp);
            }
        }
        PhysicalNode newNode = new PhysicalNode(newNodeID, ip, port, STATUS_ACTIVE, tempList);
        physicalNodes.put(newNodeID, newNode);
        long epoch = lookupTable.getEpoch();
        lookupTable.setEpoch(epoch);
        lookupTable.setPhysicalNodeMap(physicalNodes);
        newNode.setLookupTable(lookupTable);
    }
}
