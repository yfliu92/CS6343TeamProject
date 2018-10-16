package dht.rush.clusters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Cluster {
    private int id;
    private String name;
    private int epoch;
    private String ip;
    private double weight;
    private boolean isActive;
    private Map<Cluster, ArrayList<Cluster>> cachedMap;

    public Cluster(int id, String name, int epoch, String ip, double weight) {
        this.id = id;
        this.name = name;
        this.epoch = epoch;
        this.ip = ip;
        this.weight = weight;
        this.isActive = true;
        this.cachedMap = new HashMap<>();
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getEpoch() {
        return epoch;
    }

    public void setEpoch(int epoch) {
        this.epoch = epoch;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public boolean isActive() {
        return isActive;
    }

    public void setActive(boolean active) {
        isActive = active;
    }

    public Map<Cluster, ArrayList<Cluster>> getCachedMap() {
        return cachedMap;
    }

    public void setCachedMap(Map<Cluster, ArrayList<Cluster>> cachedMap) {
        this.cachedMap = cachedMap;
    }
}
