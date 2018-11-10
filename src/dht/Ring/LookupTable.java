package dht.Ring;

import java.util.*;
import java.util.Map.Entry;

import javax.json.*;

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
//		for(VirtualNode node: this.table) {
//			jsonBuilder.add(String.valueOf(node.getIndex()), node.toJSON());
//		}
		
//		JsonObject[] virtualNodes = new JsonObject[this.table.size()]; 
		
		jsonBuilder.add("epoch", this.epoch);
		
		JsonArrayBuilder tableBuilder = Json.createArrayBuilder();
//		int i = 0;
		for(VirtualNode node: this.table) {
//			jsonBuilder.add(String.valueOf(node.getIndex()), node.toJSON());
			tableBuilder.add(node.toJSON());
//			virtualNodes[i] = node.toJSON();
//			i++;
		}
		jsonBuilder.add("table", tableBuilder.build());
		
		return jsonBuilder.build();
	}
	
	public void print() {
		if (this.table == null || this.table.size() == 0) {
			System.out.println("No data found in the table");
		}
		else {
			System.out.println("epoch number: " + this.epoch);
			for(VirtualNode node: this.table) {
				System.out.println(node.serialize());
			}
		}

	}

}
