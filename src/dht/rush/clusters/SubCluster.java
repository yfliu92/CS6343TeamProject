package dht.rush.clusters;

import dht.rush.clusters.Cluster;
import dht.rush.clusters.ClusterType;

import java.util.UUID;

public class SubCluster extends Cluster {
    private ClusterType type;

    public SubCluster(String id, String ip, String port, String parentId, int numberOfChildren, double weight, Boolean isActive) {
        super(id, ip, port, parentId, numberOfChildren, weight, isActive);
        this.type = ClusterType.SUB_CLUSTER;
    }
}
