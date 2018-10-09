package dht.elastic_DHT_centralized;

import javafx.util.Pair;

import java.util.LinkedList;
import java.util.List;

public class Proxy{
    private String proxyID = "PROXY";
    private int totalHashSlots = 10000;
    private String[] routeTable;
    private List<Node> activeNodes = new LinkedList<>();

    public String getProxyID() {
        return proxyID;
    }

    public void setProxyID(String proxyID) {
        this.proxyID = proxyID;
    }

    public int getTotalHashSlots() {
        return totalHashSlots;
    }

    public void setTotalHashSlots(int totalHashSlots) {
        this.totalHashSlots = totalHashSlots;
    }

    public String[] getRouteTable() {
        return routeTable;
    }

    public void setRouteTable(String[] routeTable) {
        this.routeTable = routeTable;
    }

    public List<Node> getActiveNodes() {
        return activeNodes;
    }

    public void setActiveNodes(List<Node> activeNodes) {
        this.activeNodes = activeNodes;
    }

    public void addNode(String ip){
        String newNodeID =  "D" + ip.substring(ip.length() - 3);
        int newNumOfActiveNodes = activeNodes.size() + 1;
        int newLoadPerNode = totalHashSlots / newNumOfActiveNodes;
        int num_slots_to_migrate = newLoadPerNode / activeNodes.size();
        LinkedList<Pair<Integer,Integer>> loadForNewNode = new LinkedList<>();
        for (Node node : activeNodes){
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

        Node newNode = new Node(newNodeID, ip, loadForNewNode);
        activeNodes.add(newNode);
    }

    public void deleteNode(String nodeID){
        Node node = findNodeById(nodeID);
    }

    public Node findNodeById(String ID) {
        Node res = null;
        for(Node node : activeNodes) {
            if (node.getNodeID().equals(ID)) {
                res = node;
                break;
            }
        }

        return res;
    }
}
