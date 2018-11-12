package dht.elastic_DHT_centralized;

import java.util.HashMap;
import java.util.Map.Entry;

import javax.json.*;

public class LookupTable {
    private long epoch;
    // Integer represents the index of the hash bucket
    // HashMap<String, String> stores the replicas for that hash bucket
    private HashMap<Integer, HashMap<String, String>> bucketsTable;
    private HashMap<String, PhysicalNode> physicalNodesMap;

    public LookupTable() {
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public HashMap<Integer, HashMap<String, String>> getBucketsTable() {
        return bucketsTable;
    }

    public void setBucketsTable(HashMap<Integer, HashMap<String, String>> bucketsTable) {
        this.bucketsTable = bucketsTable;
    }

    public HashMap<String, PhysicalNode> getPhysicalNodesMap() {
        return physicalNodesMap;
    }
    
    public String[] getPhysicalNodeIds(int hash){
        Object[] replicas = bucketsTable.get(hash).keySet().toArray();
        String[] nodeids = new String[replicas.length];
        for(int i = 0; i < replicas.length; i++) {
        	nodeids[i] = replicas[i].toString();
        }
        return nodeids;
    }

    public void setPhysicalNodesMap(HashMap<String, PhysicalNode> physicalNodesMap) {
        this.physicalNodesMap = physicalNodesMap;
    }
    
	public String serialize() {
		return this.toJSON().toString();
	}

	public JsonObject toJSON() {
		JsonObjectBuilder jsonBuilder = Json.createObjectBuilder();
		jsonBuilder.add("epoch", this.epoch);
		
		JsonObjectBuilder tableBuilder = Json.createObjectBuilder();
		for(HashMap.Entry<Integer, HashMap<String, String>> bucket: this.bucketsTable.entrySet()) {
			JsonObjectBuilder hashBuilder = Json.createObjectBuilder();
			for(HashMap.Entry<String, String> pair: bucket.getValue().entrySet()) {
				hashBuilder.add(pair.getKey(), pair.getValue());
			}
			tableBuilder.add(String.valueOf(bucket.getKey()), hashBuilder.build());
		}
		jsonBuilder.add("bucketsTable", tableBuilder.build());
		
		JsonObjectBuilder physicalNodeBuilder = Json.createObjectBuilder();
		for(HashMap.Entry<String, PhysicalNode> entry: physicalNodesMap.entrySet()) {
			physicalNodeBuilder.add(entry.getKey(), entry.getValue().toJSON());
		}
		jsonBuilder.add("physicalNodesMap", physicalNodeBuilder.build());
		
		return jsonBuilder.build();
	}
	
	public boolean buildTable(JsonObject data) {
		this.bucketsTable = new HashMap<Integer, HashMap<String, String>>();
		this.physicalNodesMap = new HashMap<String, PhysicalNode>();
		if (data.containsKey("epoch")) {
			this.setEpoch(Long.valueOf(data.get("epoch").toString()));
		}
		if (data.containsKey("bucketsTable")) {
			JsonObject table = data.getJsonObject("bucketsTable");
			for(Entry<String, JsonValue> bucketJson: table.entrySet()) {
				JsonObject hashPairMap = bucketJson.getValue().asJsonObject();
				HashMap<String, String> bucket = new HashMap<String, String>();
				for(Entry<String, JsonValue> pair: hashPairMap.entrySet()) { ///?
					bucket.put(pair.getKey(), pair.getValue().toString());
				}
				this.bucketsTable.put(Integer.valueOf(bucketJson.getKey()), bucket);
			}
		}
		if (data.containsKey("physicalNodesMap")) {
			JsonObject nodes = data.getJsonObject("physicalNodesMap");
			for(Entry<String, JsonValue> node: nodes.entrySet()) {
				JsonObject nodeJson = node.getValue().asJsonObject();
				PhysicalNode physicalNode = PhysicalNode.buildPhysicalNode(nodeJson.getString("id"), nodeJson.getString("ip"), nodeJson.getInt("port"), nodeJson.getString("status"));
				this.physicalNodesMap.put(node.getKey(), physicalNode);
			}
		}
		
		return true;
	}
	
	public void print() {
		if (this.bucketsTable == null || this.bucketsTable.size() == 0) {
			System.out.println("No data found in the table");
		}
		else {
			System.out.println("Epoch number: " + this.epoch);
			System.out.println("Hash buckets: ");
			for(HashMap.Entry<Integer, HashMap<String, String>> node: this.bucketsTable.entrySet()) {
				
				System.out.print(node.getKey() + ": ");
				for(HashMap.Entry<String, String> pair: node.getValue().entrySet()) {
					System.out.print("<" + pair.getKey() + ", " + pair.getValue() + "> ");
				}
				System.out.print("\n");
			}
			System.out.println("Physical nodes: ");
			for(HashMap.Entry<String, PhysicalNode> entry: physicalNodesMap.entrySet()) {
				System.out.println(entry.getKey() + ": " + entry.getValue().serialize());
			}
		}

	}
}

