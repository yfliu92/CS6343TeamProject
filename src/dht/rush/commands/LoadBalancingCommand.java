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

public class LoadBalancingCommand extends ServerCommand {
    private Cluster root;
    private String subClusterId;
    private ClusterStructureMap clusterStructureMap;
    private CentralServer cs;

    @Override
    public void run() throws IOException {
        CommandResponse commandResponse = clusterStructureMap.loadBalancing(subClusterId);
        int status = commandResponse.getStatus();
        // "1": success
        // "2": No such a subcluster

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JsonWriter writer = Json.createWriter(baos);
        JsonObject params = null;
        if (status == 2) {
            params = Json.createObjectBuilder()
                    .add("message", "No such a sub cluster")
                    .add("status", "ERROR")
                    .build();
        } else if (status == 1) {

            JsonObjectBuilder jcb = Json.createObjectBuilder();
            jcb.add("message", "Load Balancing successfully executed, epoch:" + clusterStructureMap.getEpoch()).add("status", "OK");

            if (commandResponse.getTransferMap() != null && commandResponse.getTransferMap().size() > 0) {
                jcb.add("transferMessage", "Need to transfer files!");
                jcb.add("transferList", commandResponse.toString());
            } else {
                jcb.add("transferMessage", "No need to transfer file!");
            }
            params = jcb.build();
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
    
    public void setCentralServer(CentralServer cs) {
        this.cs = cs;
    }
}
