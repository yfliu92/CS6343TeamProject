package dht.rush.clusters;

import dht.rush.clusters.Cluster;
import dht.rush.clusters.ClusterType;

import java.util.HashMap;
import java.util.Map;

public class LeafSubCluster extends Cluster {
    public ClusterType getType() {
        return type;
    }

    public void setType(ClusterType type) {
        this.type = type;
    }

    public int getTotalSize() {
        return totalSize;
    }

    public void setTotalSize(int totalSize) {
        this.totalSize = totalSize;
    }

    public int getAvailableSize() {
        return availableSize;
    }

    public void setAvailableSize(int availableSize) {
        this.availableSize = availableSize;
    }

    public boolean isActive() {
        return isActive;
    }

    public void setActive(boolean active) {
        isActive = active;
    }

    public Map<String, Integer> getDataStore() {
        return dataStore;
    }

    public void setDataStore(Map<String, Integer> dataStore) {
        this.dataStore = dataStore;
    }

    private ClusterType type;
    private int totalSize;
    private int availableSize;
    private boolean isActive;
    // dataStore is used to simulate the file writing and reading process
    private Map<String, Integer> dataStore;

    public LeafSubCluster(int id, String name, int epoch, String ip, double weight, int size, boolean isActive) {
        super(id, name, epoch, ip, weight);
        this.type = ClusterType.LEAF_SUB_CLUSTER;
        this.totalSize = size;
        this.availableSize = size;
        this.isActive = isActive;
        this.dataStore = new HashMap<>();
    }

}
