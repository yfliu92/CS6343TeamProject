package dht.rush.clusters;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ClusterStructureMap {
    private int epoch;
    private int numberOfReplicas;
    private Map<String, Cluster> childrenList;

    public ClusterStructureMap() {
        this.childrenList = new HashMap<>();
    }

    public ClusterStructureMap(int epoch, int numberOfReplicas) {
        this.epoch = epoch;
        this.numberOfReplicas = numberOfReplicas;
        this.childrenList = new HashMap<>();
    }

    public int getEpoch() {
        return epoch;
    }

    public void setEpoch(int epoch) {
        this.epoch = epoch;
    }

    public void addEpoch() {
        this.epoch += 1;
    }

    public Map<String, Cluster> getChildrenList() {
        return childrenList;
    }

    public void setChildrenList(Map<String, Cluster> childrenList) {
        this.childrenList = childrenList;
    }

    public int getNumberOfReplicas() {
        return numberOfReplicas;
    }

    public void setNumberOfReplicas(int numberOfReplicas) {
        this.numberOfReplicas = numberOfReplicas;
    }

    public int addPhysicalNode(String subCLusterId, String ip, String port, double weight) {
        Cluster root = this.getChildrenList().get("R");
        int status = 0;
        // "1": success
        // "2": No such a subcluster
        // "3": physical node already exits
        if (root.getCachedTreeStructure().getChildrenList().containsKey(subCLusterId)) {
            Cluster sub = root.getCachedTreeStructure().getChildrenList().get(subCLusterId);
            Set<Map.Entry<String, Cluster>> set = sub.getCachedTreeStructure().getChildrenList().entrySet();

            boolean isExist = false;
            for (Map.Entry<String, Cluster> entry : set) {
                Cluster c = entry.getValue();
                String cip = c.getIp();
                String cport = c.getPort();
                if (cip.equals(ip) && cport.equals(port)) {
                    status = 3;
                    isExist = true;
                    break;
                }
            }
            if (!isExist) {
                int newId = 0;
                Set<Map.Entry<String, Cluster>> subClusterSet = root.getCachedTreeStructure().getChildrenList().entrySet();
                for (Map.Entry<String, Cluster> entry : subClusterSet) {
                    newId += entry.getValue().getNumberOfChildren();
                }
                String newClusterId = "N" + newId;
                Cluster c = new PhysicalNode(newClusterId, ip, port, subCLusterId, 0, weight, true, 100);
                sub.getCachedTreeStructure().getChildrenList().put(newClusterId, c);
                this.addEpoch();
                status = 1;
            }
        } else {
            System.out.println("No such subcluster");
            status = 2;
        }
        return status;
    }

    public int deletePhysicalNode(String subCLusterId, String ip, String port) {
        Cluster root = this.getChildrenList().get("R");
        int status = 0;

        // "1": success delete
        // "2": No such a sub cluster
        // "3": No such a physical node in the specific subcluster
        // "4": Found the node, but already inactive

        if (root.getCachedTreeStructure().getChildrenList().containsKey(subCLusterId)) {
            Cluster sub = root.getCachedTreeStructure().getChildrenList().get(subCLusterId);
            Set<Map.Entry<String, Cluster>> set = sub.getCachedTreeStructure().getChildrenList().entrySet();

            boolean isExist = false;
            for (Map.Entry<String, Cluster> entry : set) {
                Cluster c = entry.getValue();
                String cip = c.getIp();
                String cport = c.getPort();
                if (cip.equals(ip) && cport.equals(port)) {
                    isExist = true;
                    if(c.getActive()) {
                        c.setActive(false);
                        status = 1;
                        this.addEpoch();
                    } else {
                        status = 4;
                    }
                    break;
                }
            }
            if (!isExist) {
                status = 3;
            }
        } else {
            System.out.println("No such subcluster");
            status = 2;
        }
        return status;
    }
}
