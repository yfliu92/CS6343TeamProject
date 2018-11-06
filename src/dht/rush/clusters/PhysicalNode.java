package dht.rush.clusters;

import dht.rush.clusters.Cluster;
import dht.rush.clusters.ClusterType;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class PhysicalNode extends Cluster {
    private ClusterType type;
    private int totalSize;
    private int availableSize;

    // dataStore is used to simulate the file writing and reading process
    private Map<String, Integer> dataStore;

    public PhysicalNode(String id, String ip, String port, String parentId, int numberOfChildren, double weight, Boolean isActive, int totalSize) {
        super(id, ip, port, parentId, numberOfChildren, weight, isActive);
        this.totalSize = totalSize;
        this.availableSize = totalSize;
        this.type = ClusterType.PHYSICAL_NODE;
        this.dataStore = new HashMap<>();
    }

    public ClusterType getType() {
        return type;
    }

    public void setType(ClusterType type) {
        this.type = type;
    }
}
