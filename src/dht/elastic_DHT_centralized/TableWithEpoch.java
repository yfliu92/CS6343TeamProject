package dht.elastic_DHT_centralized;

import dht.elastic_DHT_centralized.algorithm.ReplicaPlacementAlgorithm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TableWithEpoch {
    private long epoch;
    private ArrayList<List<String>>  table;
    private HashMap<String, PhysicalNode> physicalNodes;
    private ReplicaPlacementAlgorithm algorithm;

    public TableWithEpoch(){
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public ArrayList<List<String>> getTable() {
        return table;
    }

    public void setTable(ArrayList<List<String>> table) {
        this.table = table;
    }

    public HashMap<String, PhysicalNode> getPhysicalNodes() {
        return physicalNodes;
    }

    public void setPhysicalNodes(HashMap<String, PhysicalNode> physicalNodes) {
        this.physicalNodes = physicalNodes;
    }

    public ReplicaPlacementAlgorithm getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(ReplicaPlacementAlgorithm algorithm) {
        this.algorithm = algorithm;
    }
}
