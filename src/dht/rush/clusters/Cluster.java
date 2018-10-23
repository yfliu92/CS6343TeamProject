package dht.rush.clusters;

import java.util.*;

public class Cluster {
    private String id;  // ip:port
    private String ip;
    private String port;
    private int numberOfChildren;
    private double weight;
    private Boolean isActive;
    private String parentId;

    private ClusterStructureMap cachedTreeStructure;

    public Cluster() {
        this.cachedTreeStructure = new ClusterStructureMap();
    }

    public Cluster(String id, String ip, String port, String parentId, int numberOfChildren, double weight, Boolean isActive) {
        this.id = id;
        this.ip = ip;
        this.port = port;
        this.parentId = parentId;
        this.numberOfChildren = numberOfChildren;
        this.weight = weight;
        this.isActive = isActive;
        this.cachedTreeStructure = new ClusterStructureMap();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }


    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public int getNumberOfChildren() {
        return numberOfChildren;
    }

    public void setNumberOfChildren(int numberOfChildren) {
        this.numberOfChildren = numberOfChildren;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public Boolean getActive() {
        return isActive;
    }

    public void setActive(Boolean active) {
        isActive = active;
    }

    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    public ClusterStructureMap getCachedTreeStructure() {
        return cachedTreeStructure;
    }

    public void setCachedTreeStructure(ClusterStructureMap cachedTreeStructure) {
        this.cachedTreeStructure = cachedTreeStructure;
    }
}
