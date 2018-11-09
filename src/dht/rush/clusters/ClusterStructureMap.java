package dht.rush.clusters;

import dht.rush.commands.CommandResponse;
import dht.rush.utils.RushUtil;
import dht.server.Command;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.*;

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

    public CommandResponse addPhysicalNode(String subCLusterId, String ip, String port, double weight) {
        Cluster root = this.getChildrenList().get("R");

        CommandResponse commandResponse = new CommandResponse();
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
                    commandResponse.setStatus(3);
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
                sub.getSubClusters().add(c);
                sub.setNumberOfChildren(sub.getNumberOfChildren() + 1);
                this.addEpoch();
                commandResponse.setStatus(1);
                commandResponse.setTransferMap(transferedFileInSubCluster(sub));
            }
        } else {
            System.out.println("No such subcluster");
            commandResponse.setStatus(2);
        }
        return commandResponse;
    }

    public CommandResponse deletePhysicalNode(String subCLusterId, String ip, String port) {
        Cluster root = this.getChildrenList().get("R");
        CommandResponse ret = new CommandResponse();
        ret.setStatus(0);

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
                    if (c.getActive()) {
                        c.setActive(false);

                        ret.setTransferMap(transferedMap(c));
                        ret.setStatus(1);
                        this.addEpoch();
                    } else {
                        ret.setStatus(4);
                    }
                    break;
                }
            }
            if (!isExist) {
                ret.setStatus(3);
            }
        } else {
            System.out.println("No such subcluster");
            ret.setStatus(2);
        }
        return ret;
    }

    public Map<Integer, Cluster> getNodes(String pgid) {
        int r = 0;
        int count = 0;
        Map<Integer, Cluster> ret = new HashMap<>();
        Map<String, Cluster> map = new HashMap<>();

        while (count < 3) {
            Cluster cluster = rush(pgid, r);
            if (cluster != null && cluster.getActive() && !map.containsKey(cluster.getId())) {
                count += 1;
                cluster.getPlacementGroupMap().put(pgid, r);
                ret.put(r, cluster);
                map.put(cluster.getId(), cluster);
                System.out.println("Replica: " + r + ", Node: " + cluster.toString());
            }
            r++;
        }

        return ret;
    }

    /**
     * Based on the placementGroupID and r, get the cluster(physical node)
     *
     * @param placementGroupID
     * @param r
     * @return Cluster
     */
    public Cluster rush(String placementGroupID, int r) {
        Cluster root = this.getChildrenList().get("R");
        Queue<Cluster> queue = new LinkedList<>();
        queue.add(root);

        while (!queue.isEmpty()) {
            Cluster node = queue.poll();

            List<Cluster> list = node.getSubClusters();
            for (int i = 0; i < list.size(); i++) {
                Cluster child = list.get(i);
                if (child == null || !child.getActive()) {
                    continue;
                }
                double rushHash = RushUtil.rushHash(placementGroupID, r, child.getId());
                double ratio = weightRatio(i, list);

                if (rushHash < ratio) {
                    if (child instanceof PhysicalNode) {
                        return child;
                    } else {
                        queue.add(child);
                        break;
                    }
                }
            }

        }

        return null;
    }

    private double weightRatio(int i, List<Cluster> children) {
        double sum = 0;

        for (int index = i; index < children.size(); index++) {
            Cluster cluster = children.get(index);
            if (cluster.getActive()) {
                sum += cluster.getWeight();
            }
        }

        return sum == 0 ? 1 : children.get(i).getWeight() / sum;
    }

    /**
     * @param c
     * @return String, Cluster -- String: pg id, Cluster: destination cluster
     */
    public Map<String, Cluster[]> transferedMap(Cluster c) {
        Map<String, Cluster[]> ret = null;

        if (c.getPlacementGroupMap().size() > 0) {
            ret = new HashMap<>();
            for (Map.Entry<String, Integer> entry : c.getPlacementGroupMap().entrySet()) {
                String id = entry.getKey();
                Integer replica = entry.getValue();

                Cluster destination = rush(id, replica);

                if (!destination.getPlacementGroupMap().containsKey(id)) {
                    destination.getPlacementGroupMap().put(id, replica);
                    ret.put(id, new Cluster[]{c, destination});
                }
            }
        }

        return ret;
    }

    public Map<String, Cluster[]> transferedFileInSubCluster(Cluster sub) {
        Map<String, Cluster[]> ret = new HashMap<>();

        for (Cluster c : sub.getSubClusters()) {
            ret.putAll(transferedMap(c));
        }

        return ret;
    }

    public CommandResponse loadBalancing(String subClusterId) {
        Cluster root = this.getChildrenList().get("R");
        CommandResponse commandResponse = new CommandResponse();

        // "1": success
        // "2": No such a subcluster

        if (root.getCachedTreeStructure().getChildrenList().containsKey(subClusterId)) {
            Cluster sub = root.getCachedTreeStructure().getChildrenList().get(subClusterId);
            commandResponse.setStatus(1);
            commandResponse.setTransferMap(transferedFileInSubCluster(sub));
        } else {
            commandResponse.setStatus(2);
        }

        return commandResponse;
    }

    /**
     * @param fileName
     * @return
     */
    public Map<Integer, Cluster> write(String fileName) {

        String pgid = generatePlacementGroupId(fileName);
        Map<Integer, Cluster> nodes = getNodes(pgid);

        return nodes;
    }

    /**
     * Find a cluster by filename
     *
     * @param fileName
     * @return Cluster
     */
    public Cluster read(String fileName) {
        String pgid = generatePlacementGroupId(fileName);

        int r = 0;
        while (true) {
            Cluster node = rush(pgid, r++);
            if (node != null && node.getActive() && node instanceof PhysicalNode)
                return node;
        }
    }

    /**
     * Generate pg id for the file name
     *
     * @param str
     * @return
     */
    public String generatePlacementGroupId(String str) {
        int pgid = RushUtil.positiveHash(str.hashCode()) % RushUtil.NUMBER_OF_PLACEMENT_GROUP;
        return "PG" + pgid;
    }

    /**
     *
     * @return
     * The most recent cluster structure map
     */

    public JsonObject getClusterMap() {
        JsonObjectBuilder jcb = Json.createObjectBuilder();

        Cluster root = this.getChildrenList().get("R");
        Queue<Cluster> queue = new LinkedList<>();
        queue.add(root);

        while (!queue.isEmpty()) {
            Cluster node = queue.poll();



            List<Cluster> list = node.getSubClusters();
            for (int i = 0; i < list.size(); i++) {
                Cluster child = list.get(i);

            }

        }


        JsonObject jsonObject = jcb.build();
        return jsonObject;
    }
}
