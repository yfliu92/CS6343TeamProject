package dht.rush.clusters;

import dht.rush.clusters.Cluster;
import dht.rush.clusters.ClusterType;

import java.util.HashMap;
import java.util.Map;

public class LeafSubCluster extends Cluster {
    private ClusterType type;
    private int totalSize;
    private int availableSize;
    // dataStore is used to simulate the file writing and reading process
    private Map<String, Integer> dataStore;

    public LeafSubCluster(int id, String name, int epoch, String ip, double weight, int size) {
        super(id, name, epoch, ip, weight);
        this.type = ClusterType.LEAF_SUB_CLUSTER;
        this.totalSize = size;
        this.availableSize = size;
        this.dataStore = new HashMap<>();
    }

}
