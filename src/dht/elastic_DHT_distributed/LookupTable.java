package dht.elastic_DHT_distributed;

import java.util.ArrayList;
import java.util.HashMap;

public class LookupTable {
    private long epoch;
    private ArrayList<HashMap<String, String>> bucketsTable;
    private HashMap<String, PhysicalNode> physicalNodesMap;

    public LookupTable() {
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public ArrayList<HashMap<String, String>> getBucketsTable() {
        return bucketsTable;
    }

    public void setBucketsTable(ArrayList<HashMap<String, String>> bucketsTable) {
        this.bucketsTable = bucketsTable;
    }

    public HashMap<String, PhysicalNode> getPhysicalNodesMap() {
        return physicalNodesMap;
    }

    public void setPhysicalNodesMap(HashMap<String, PhysicalNode> physicalNodesMap) {
        this.physicalNodesMap = physicalNodesMap;
    }
}

