package dht.rush.commands;

import dht.rush.clusters.Cluster;
import dht.rush.clusters.ClusterStructureMap;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class DeleteNodeCommand extends ServerCommand {
    private Cluster root;
    private String ip;
    private String port;
    private Double weight;
    private String subClusterId;
    private ClusterStructureMap clusterStructureMap;

    @Override
    public void run() throws IOException {
        int status = clusterStructureMap.deletePhysicalNode(subClusterId, ip, port);

        // "1": success delete
        // "2": No such a sub cluster
        // "3": No such a physical node in the specific subcluster
        // "4": Found the node, but already inactive

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JsonWriter writer = Json.createWriter(baos);
        JsonObject params = null;
        if (status == 1) {
            params = Json.createObjectBuilder()
                    .add("message", "Delete success :" + clusterStructureMap.getEpoch())
                    .add("status", "OK")
                    .build();
        } else if (status == 2) {
            params = Json.createObjectBuilder()
                    .add("message", "No such a subcluster")
                    .add("status", "ERROR")
                    .build();
        } else if (status == 3) {
            params = Json.createObjectBuilder()
                    .add("message", "The node isn't in the sub cluster.")
                    .add("status", "ERROR")
                    .build();
        } else if (status == 4) {
            params = Json.createObjectBuilder()
                    .add("message", "The node is inactive.")
                    .add("status", "ERROR")
                    .build();
        }
        writer.writeObject(params);
        writer.close();
        baos.writeTo(outputStream);
        outputStream.write("\n".getBytes());
        outputStream.flush();
    }

    public Cluster getRoot() {
        return root;
    }

    public void setRoot(Cluster root) {
        this.root = root;
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

    public Double getWeight() {
        return weight;
    }

    public void setWeight(Double weight) {
        this.weight = weight;
    }

    public String getSubClusterId() {
        return subClusterId;
    }

    public void setSubClusterId(String subClusterId) {
        this.subClusterId = subClusterId;
    }

    public ClusterStructureMap getClusterStructureMap() {
        return clusterStructureMap;
    }

    public void setClusterStructureMap(ClusterStructureMap clusterStructureMap) {
        this.clusterStructureMap = clusterStructureMap;
    }
}
