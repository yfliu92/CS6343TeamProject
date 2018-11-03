package dht.rush.commands;

import dht.rush.clusters.Cluster;
import dht.rush.clusters.ClusterStructureMap;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class ReadCommand extends ServerCommand {
    private String fileName;
    private ClusterStructureMap clusterStructureMap;

    @Override
    public void run() throws IOException {

        Cluster ret = clusterStructureMap.read(fileName);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JsonWriter writer = Json.createWriter(baos);

        JsonObject params = Json.createObjectBuilder().add("destination: ", ret.toString()).build();

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

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public ClusterStructureMap getClusterStructureMap() {
        return clusterStructureMap;
    }

    public void setClusterStructureMap(ClusterStructureMap clusterStructureMap) {
        this.clusterStructureMap = clusterStructureMap;
    }
}
