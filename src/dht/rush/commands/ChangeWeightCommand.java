package dht.rush.commands;

import dht.rush.CentralServer;
import dht.rush.clusters.Cluster;
import dht.rush.clusters.ClusterStructureMap;
import dht.rush.utils.RushUtil;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ChangeWeightCommand extends ServerCommand {
    private Cluster root;
    private String ip;
    private String port;
    private Double weight;
    private String subClusterId;
    private ClusterStructureMap clusterStructureMap;
    private CentralServer cs;

    @Override
    public void run() throws IOException {
        CommandResponse commandResponse = clusterStructureMap.changeNodeWeight(subClusterId, ip, port, weight);

        int status = commandResponse.getStatus();
        // "1": success change
        // "2": No such a sub cluster
        // "3": No such a physical node in the specific subcluster


        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JsonWriter writer = Json.createWriter(baos);
        JsonObject params = null;
        if (status == 1) {
            JsonObjectBuilder jcb = Json.createObjectBuilder();
            JsonObject ret = clusterStructureMap.getClusterMap();
            jcb.add("message", "Change weight success, epoch:" + clusterStructureMap.getEpoch()).add("status", "OK");
            jcb.add("epoch", ret.get("epoch")).add("nodes", ret.getJsonObject("nodes"));

            if (commandResponse.getTransferMap() != null && commandResponse.getTransferMap().size() > 0) {
                jcb.add("transferMessage", "Need to transfer files!");
                jcb.add("transferList", commandResponse.toString());
            } else {
                jcb.add("transferMessage", "No need to transfer file!");
            }
            params = jcb.build();

        } else if (status == 2) {
            params = Json.createObjectBuilder()
                    .add("message", "No such a subcluster")
                    .add("status", "ERROR， " + "epoch: " + clusterStructureMap.getEpoch())
                    .build();
        } else if (status == 3) {
            params = Json.createObjectBuilder()
                    .add("message", "The node isn't in the sub cluster.")
                    .add("status", "ERROR, " + "epoch: " + clusterStructureMap.getEpoch())
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
        } else {
            System.out.println("Response Sent");
        }
        
        if (status == 1) {
    		System.out.println("Beginning to push DHT to all physical nodes --- " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));

        	synchronized(this.cs) {
        		this.cs.initializeDataNode(this.cs.getRoot());
        	}
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

    public void setWeight(double weight) {
        this.weight = weight;
    }
    
    public void setCentralServer(CentralServer cs) {
        this.cs = cs;
    }
}
