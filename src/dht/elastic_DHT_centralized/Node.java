package dht.elastic_DHT_centralized;

import javafx.util.Pair;

import java.util.LinkedList;

public class Node {
    private String nodeID;
    private String IP;
    private LinkedList<Pair<Integer,Integer>> currentLoad;
    private Proxy proxy;


    public Node(String ID, String ip, LinkedList<Pair<Integer,Integer>> load){
        this.nodeID = ID;
        this.IP = ip;
        this.currentLoad = load;
    }

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

    public Proxy getProxy() {
        return proxy;
    }

    public void setProxy(Proxy proxy) {
        this.proxy = proxy;
    }

    public LinkedList<Pair<Integer, Integer>> getCurrentLoad() {
        return currentLoad;
    }

    public void updateLoad(LinkedList<Pair<Integer, Integer>> currentLoad) {
        this.currentLoad = currentLoad;
    }


}
