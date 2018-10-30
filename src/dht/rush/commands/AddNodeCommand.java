package dht.rush.commands;

import dht.rush.clusters.Cluster;
import dht.rush.clusters.ClusterStructureMap;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AddNodeCommand extends ServerCommand {
    private Cluster root;
    private String ip;
    private String port;
    private Double weight;
    private String subClusterId;
    private ClusterStructureMap clusterStructureMap;

    @Override
    public void run() throws IOException {
        int status = clusterStructureMap.addPhysicalNode(subClusterId, ip, port, weight);
        // "1": success
        // "2": No such a subcluster
        // "3": physical node already exits
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JsonWriter writer = Json.createWriter(baos);
        JsonObject params = null;
        if (status == 1) {
            params = Json.createObjectBuilder()
                    .add("message", "Add Node Success, new epoch is :" + clusterStructureMap.getEpoch())
                    .add("status", "OK")
                    .build();
        } else if (status == 2) {
            params = Json.createObjectBuilder()
                    .add("message", "No such a subcluster")
                    .add("status", "ERROR")
                    .build();
        } else if (status == 3) {
            params = Json.createObjectBuilder()
                    .add("message", "The sub cluster already has the node.")
                    .add("status", "ERROR")
                    .build();
        }
        writer.writeObject(params);
        writer.close();
        baos.writeTo(outputStream);
        outputStream.write("\n".getBytes());
        outputStream.flush();
        
        System.out.println();
        if (params != null) {
            System.out.println("Response Sent -- " + params.toString());
            System.out.println("REPONSE STATUS: " + params.getString("status") + ", " + "message: " + params.getString("message"));
        }
        else {
        	System.out.println("Response Sent");
        }
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
