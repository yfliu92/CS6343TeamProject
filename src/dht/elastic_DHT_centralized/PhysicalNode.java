package dht.elastic_DHT_centralized;

import java.util.HashMap;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

public class PhysicalNode {

    private String id;

    private String ip;

    private int port;

    private String status; // "active", "inactive"

    private LookupTable lookupTable;

    public PhysicalNode() {
    }

    public PhysicalNode (String ip, int port, String status){
        this.id = ip + "-" + Integer.toString(port);
        this.ip = ip;
        this.port = port;
        this.status = status;
        this.lookupTable = new LookupTable();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public LookupTable getLookupTable() {
        return lookupTable;
    }

    public void setLookupTable(LookupTable lookupTable) {
        this.lookupTable = lookupTable;
    }

	public String serialize() {
		return this.toJSON().toString();
	}

	public JsonObject toJSON() {
		JsonObjectBuilder jsonBuilder = Json.createObjectBuilder();
		jsonBuilder.add("id", this.id);
		jsonBuilder.add("ip", this.ip);
		jsonBuilder.add("port", this.port);
		jsonBuilder.add("status", this.status);
		
		return jsonBuilder.build();
	}
	
	public static PhysicalNode buildPhysicalNode(String id, String ip, int port, String status) {
		PhysicalNode node = new PhysicalNode();
		node.id = id;
		node.ip = ip;
		node.port = port;
		node.status = status;
		return node;
    }

}

