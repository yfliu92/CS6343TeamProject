package dht.rush.commands;

import dht.common.response.Response;
import dht.rush.CentralServer;
import dht.rush.clusters.ClusterStructureMap;

import javax.json.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

public class GetDHTCommand extends ServerCommand {
    private ClusterStructureMap clusterStructureMap;
    private CentralServer cs;
    private String operation;
    private List<String> commandSeries;

    @Override
    public void run() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JsonWriter writer = Json.createWriter(baos);
        JsonObjectBuilder paramsBuilder =  Json.createObjectBuilder();
        
        JsonObject params = null;
        if (operation.equalsIgnoreCase("pull")) {
        	params = new Response(true, clusterStructureMap.toJSON(), "Rush DHT table").toJSON();
        }
        else if (operation.equalsIgnoreCase("head")) {
        	params = new Response(true, String.valueOf(clusterStructureMap.getEpoch()), "Current epoch number:").toJSON();
        }
        else if (operation.equalsIgnoreCase("print")) {
        	clusterStructureMap.print();
        	params = new Response(true, "DHT printed on server").toJSON();
        }
        else if (operation.equalsIgnoreCase("push")) {

			if (commandSeries.size() == 3) {
				String ip = commandSeries.get(1);
				int port = Integer.valueOf(commandSeries.get(2));
				cs.pushDHT(ip, port, cs.hashRange);
				params = new Response(true, "DHT pushed for " + ip + " " + port).toJSON();
			}
			else if (commandSeries.size() == 1) {
				params = new Response(true, "DHT push to all nodes is being executed").toJSON();
			}
			else {
				params = new Response(false, "DHT not pushed").toJSON();
			}
        }
        else {
        	params = new Response(false, "Command not supported").toJSON();
        }

//        paramsBuilder.add("status", "OK");
//        JsonObject params = paramsBuilder.build();
                

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
        
        if (operation.equalsIgnoreCase("push") && commandSeries.size() == 1) {
        	this.cs.initializeDataNode(this.cs.getRoot());
        }

    }

    public ClusterStructureMap getClusterStructureMap() {
        return clusterStructureMap;
    }

    public void setClusterStructureMap(ClusterStructureMap clusterStructureMap) {
        this.clusterStructureMap = clusterStructureMap;
    }
    
    public String getOperation() {
        return operation;
    }

    public void setOperation(String fetchtype) {
        this.operation = fetchtype;
    }
    
    public List<String> getCommandSeries() {
        return commandSeries;
    }

    public void setCommandSeries(List<String> commandSeries) {
        this.commandSeries = commandSeries;
    }

    public void setCentralServer(CentralServer cs) {
        this.cs = cs;
    }
}
