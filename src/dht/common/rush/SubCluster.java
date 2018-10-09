package dht.common.rush;

import java.util.Map;

public class SubCluster {
    private int id;
    private int m;               // number of disks in the cluster
    private double w;            // weight of each disk
    private Map<Integer, RushOSD> OSDs;
}
