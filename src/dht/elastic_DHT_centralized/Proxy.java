package dht.elastic_DHT_centralized;

import javafx.util.Pair;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class Proxy extends PhysicalNode{
    private String id = "PROXY";
    private TableWithEpoch table;
    private HashMap<Integer, PhysicalNode> members = new HashMap<>();

    public String getProxyID() {
        return proxyID;
    }

    public void setProxyID(String proxyID) {
        this.proxyID = proxyID;
    }

    public void setProxy(PhysicalNode proxy) {
        this.proxy = proxy;
    }

    public void addNode(String ip){
        String newNodeID =  "D" + ip.substring(ip.length() - 3);

    }
    public void add(List<PhysicalNode> nodes){

    }
    public void remove(PhysicalNode node){

    }
    public void remove(List<PhysicalNode> nodes){

    }

    public void addNode(String ip){

        int newNumOfActiveNodes = activeNodes.size() + 1;
        int newLoadPerNode = totalHashSlots / newNumOfActiveNodes;
        int num_slots_to_migrate = newLoadPerNode / activeNodes.size();
        LinkedList<Pair<Integer,Integer>> loadForNewNode = new LinkedList<>();
        for (PhysicalNode node : activeNodes){
            int count = num_slots_to_migrate;
            LinkedList<Pair<Integer,Integer>> newLoad = new LinkedList<>();
            for (Pair<Integer, Integer> pair : node.getCurrentLoad()){
                if (count > 0){
                    int start = pair.getKey();
                    int end = pair.getValue();
                    if (end - start > count){
                        for (int i = start; i < start + count; i++){
                            routeTable[i] = newNodeID;
                        }
                        // Get a pair of(start, end) for the newly added node
                        // The newly added node is assigned hash slots [start, end)
                        Pair pr = new Pair(start, start + count);
                        loadForNewNode.add(pr);
                        // Update a pair of (start, end) for each old node
                        start = start + count;
                        Pair newPr = new Pair(start, end);
                        newLoad.add(newPr);
                        count = 0;
                    }
                    else {
                        count -= (end - start);
                    }
                }
                else{
                    newLoad.add(pair);
                }
            }
            node.updateLoad(newLoad);
        }

        PhysicalNode newNode = new PhysicalNode(newNodeID, ip, loadForNewNode);
        activeNodes.add(newNode);
    }

    public void deleteNode(String nodeID){
        PhysicalNode node = findNodeById(nodeID);
    }

    public PhysicalNode findNodeById(String ID) {
        PhysicalNode res = null;
        for(PhysicalNode node : activeNodes) {
            if (node.getNodeID().equals(ID)) {
                res = node;
                break;
            }
        }

        return res;
    }
}
