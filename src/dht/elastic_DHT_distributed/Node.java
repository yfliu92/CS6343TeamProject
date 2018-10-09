package dht.elastic_DHT_distributed;

import javafx.util.Pair;

import java.util.LinkedList;
import java.util.List;

public class Node {
    private String nodeID;
    private String IP;
    private LinkedList<Pair<Integer,Integer>> currentLoad;
    private List<Node> neighbors;
    private String[] routeTable;

    public String getNodeID() {
        return nodeID;
    }

    public void setNodeID(String nodeID) {
        this.nodeID = nodeID;
    }

    public String getIP() {
        return IP;
    }

    public void setIP(String IP) {
        this.IP = IP;
    }

    public List<Node> getNeighbors() {
        return neighbors;
    }

    public void setNeighbors(List<Node> neighbors) {
        this.neighbors = neighbors;
    }

    public LinkedList<Pair<Integer, Integer>> getCurrentLoad() {
        return currentLoad;
    }

    public void setCurrentLoad(LinkedList<Pair<Integer, Integer>> currentLoad) {
        this.currentLoad = currentLoad;
    }

    public String[] getRouteTable() {
        return routeTable;
    }

    public void setRouteTable(String[] routeTable) {
        this.routeTable = routeTable;
    }

    public Node(String ID, String ip, LinkedList<Pair<Integer,Integer>> load){
        this.nodeID = ID;
        this.IP = ip;
        this.currentLoad = load;
    }



}

