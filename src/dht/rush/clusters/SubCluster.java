package dht.rush.clusters;

import dht.rush.clusters.Cluster;
import dht.rush.clusters.ClusterType;

public class SubCluster extends Cluster {
    private ClusterType type;

    public SubCluster(int id, String name, int epoch, String ip, double weight) {
        super(id, name, epoch, ip, weight);
        this.type = ClusterType.SUB_CLUSTER;
    }
}
