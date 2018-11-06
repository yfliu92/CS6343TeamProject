package dht.rush.commands;

import dht.rush.clusters.Cluster;
import dht.rush.clusters.ClusterStructureMap;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetNodesCommand extends ServerCommand {

    private ClusterStructureMap clusterStructureMap;
    private String pgid;

    @Override
    public void run() throws IOException {

        Map<Integer, Cluster> ret = clusterStructureMap.getNodes(pgid);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JsonWriter writer = Json.createWriter(baos);
        JsonObject params;
        JsonObjectBuilder jcb = Json.createObjectBuilder();

        for (Map.Entry<Integer, Cluster> entry : ret.entrySet()) {
            jcb.add(String.valueOf(entry.getKey()), entry.getValue().toString());
        }
        params = jcb.build();

        writer.writeObject(params);
        writer.close();
        baos.writeTo(outputStream);
        outputStream.write("\n".getBytes());
        outputStream.flush();

        System.out.println();
        if (params != null) {
            System.out.println("Response Sent -- " + params.toString());
        } else {
            System.out.println("Response Sent");
        }
    }

    public String getPgid() {
        return pgid;
    }

    public void setPgid(String pgid) {
        this.pgid = pgid;
    }

    public ClusterStructureMap getClusterStructureMap() {
        return clusterStructureMap;
    }

    public void setClusterStructureMap(ClusterStructureMap clusterStructureMap) {
        this.clusterStructureMap = clusterStructureMap;
    }
}
