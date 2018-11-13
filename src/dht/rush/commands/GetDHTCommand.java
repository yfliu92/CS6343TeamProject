package dht.rush.commands;

import dht.rush.clusters.ClusterStructureMap;

import javax.json.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class GetDHTCommand extends ServerCommand {
    private ClusterStructureMap clusterStructureMap;
    private String fetchtype;

    @Override
    public void run() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JsonWriter writer = Json.createWriter(baos);
        JsonObjectBuilder paramsBuilder =  Json.createObjectBuilder();
        
        JsonObject ret = Json.createObjectBuilder().build();
        if (fetchtype.equalsIgnoreCase("pull")) {
        	ret = clusterStructureMap.toJSON();
        	paramsBuilder.add("jsonResult", ret);
        }
        else if (fetchtype.equalsIgnoreCase("head")) {
        	ret = clusterStructureMap.info();
        	paramsBuilder.add("jsonResult", ret);
        }
        else if (fetchtype.equalsIgnoreCase("print")) {
        	clusterStructureMap.print();
        }

        paramsBuilder.add("status", "OK");
        JsonObject params = paramsBuilder.build();
                

        writer.writeObject(params);
        writer.close();
        baos.writeTo(outputStream);
        outputStream.write("\n".getBytes());
        outputStream.flush();
        
        System.out.println();
        if (params != null) {
            System.out.println("Response Sent -- " + params.toString());
//            System.out.println("REPONSE STATUS: " + params.getString("status") + ", " + "message: " + params.getString("message"));
        } else {
            System.out.println("Response Sent");
        }

    }


    public ClusterStructureMap getClusterStructureMap() {
        return clusterStructureMap;
    }

    public void setClusterStructureMap(ClusterStructureMap clusterStructureMap) {
        this.clusterStructureMap = clusterStructureMap;
    }
    
    public String getFetchType() {
        return fetchtype;
    }

    public void setFetchType(String fetchtype) {
        this.fetchtype = fetchtype;
    }
}
