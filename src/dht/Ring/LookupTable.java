package dht.Ring;

import java.util.*;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

public class LookupTable {

    private long epoch;

    private BinarySearchList table;

    private HashMap<String, PhysicalNode> physicalNodeMap;


    public LookupTable() {
    	this.table = new BinarySearchList();
    	this.physicalNodeMap = new HashMap<>();
    }


    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public BinarySearchList getTable() {
        return table;
    }

    public void setTable(BinarySearchList table) {
        this.table = table;
    }

    public HashMap<String, PhysicalNode> getPhysicalNodeMap() {
        return physicalNodeMap;
    }

    public void setPhysicalNodeMap(HashMap<String, PhysicalNode> physicalNodeMap) {
        this.physicalNodeMap = physicalNodeMap;
    }
    
	public String serialize() {
		return this.toJSON().toString();
	}

	public JsonObject toJSON() {
		JsonObjectBuilder jsonBuilder = Json.createObjectBuilder();
		for(VirtualNode node: this.table) {
			jsonBuilder.add(String.valueOf(node.getIndex()), node.toJSON());
		}
		return jsonBuilder.build();
	}
	
	public void print() {
		for(VirtualNode node: this.table) {
			System.out.println(node.serialize());
		}
	}

}
