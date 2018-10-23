package dht.Ring;

import java.util.HashMap;

public class LookupTable {

    private long epoch;

    private BinarySearchList ring;

    private HashMap<String, PhysicalNode> physicalNodeMap;

    public LookupTable() {
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public BinarySearchList getRing() {
        return ring;
    }

    public void setRing(BinarySearchList ring) {
        this.ring = ring;
    }

    public HashMap<String, PhysicalNode> getPhysicalNodeMap() {
        return physicalNodeMap;
    }

    public void setPhysicalNodeMap(HashMap<String, PhysicalNode> physicalNodeMap) {
        this.physicalNodeMap = physicalNodeMap;
    }


}
