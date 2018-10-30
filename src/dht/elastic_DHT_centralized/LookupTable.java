package dht.elastic_DHT_centralized;

import java.util.ArrayList;
import java.util.HashMap;

public class LookupTable {
    private long epoch;
    // Integer represents the index of the hash bucket
    // HashMap<String, String> stores the replicas for that hash bucket
    private HashMap<Integer, HashMap<String, String>> bucketsTable;
    private HashMap<String, PhysicalNode> physicalNodesMap;

    public LookupTable() {
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public HashMap<Integer, HashMap<String, String>> getBucketsTable() {
        return bucketsTable;
    }

    public void setBucketsTable(HashMap<Integer, HashMap<String, String>> bucketsTable) {
        this.bucketsTable = bucketsTable;
    }

    public HashMap<String, PhysicalNode> getPhysicalNodesMap() {
        return physicalNodesMap;
    }

    public void setPhysicalNodesMap(HashMap<String, PhysicalNode> physicalNodesMap) {
        this.physicalNodesMap = physicalNodesMap;
    }
}

