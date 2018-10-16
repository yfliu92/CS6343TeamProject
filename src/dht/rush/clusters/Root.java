package dht.rush.clusters;

import dht.rush.clusters.Cluster;
import dht.rush.clusters.ClusterType;

// Root node is the central server
public class Root extends Cluster {
    private ClusterType type;

    public Root(int id, String name, int epoch, String ip, double weight) {
        super(id, name, epoch, ip, weight);
        this.type = ClusterType.ROOT;
    }
}
