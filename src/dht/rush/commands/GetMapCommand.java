package dht.rush.commands;

import dht.rush.clusters.Cluster;
import dht.rush.clusters.ClusterStructureMap;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class GetMapCommand extends ServerCommand {
    private ClusterStructureMap clusterStructureMap;

    @Override
    public void run() throws IOException {
        JsonObject ret = clusterStructureMap.getClusterMap();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JsonWriter writer = Json.createWriter(baos);
        JsonObject params =  Json.createObjectBuilder()
                .add("epoch", ret.get("epoch"))
                .add("nodes", ret.getJsonObject("nodes"))
                .add("status", "OK")
                .build();

        writer.writeObject(params);
        writer.close();
        baos.writeTo(outputStream);
        outputStream.write("\n".getBytes());
        outputStream.flush();

    }


    public ClusterStructureMap getClusterStructureMap() {
        return clusterStructureMap;
    }

    public void setClusterStructureMap(ClusterStructureMap clusterStructureMap) {
        this.clusterStructureMap = clusterStructureMap;
    }
}
