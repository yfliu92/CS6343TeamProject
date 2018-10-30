package dht.rush.clusters;

// Root node is the central server
public class Root extends Cluster {
    private ClusterType type;

    public Root(String id, String ip, String port, String parentId, int numberOfChildren, double weight, Boolean isActive) {
        super(id, ip, port, parentId, numberOfChildren, weight, isActive);
        this.type = ClusterType.ROOT;
    }

    public ClusterType getType() {
        return type;
    }

    public void setType(ClusterType type) {
        this.type = type;
    }
}
