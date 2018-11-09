package dht.Ring;

import javax.json.Json;
import javax.json.JsonObject;

public class VirtualNode implements Comparable<VirtualNode> {

    private int hash;

    private int index;

    private String physicalNodeId;

    public VirtualNode() {
        this.index = -1;
    }

    public VirtualNode(int hash) {
        this();
        this.hash = hash;
    }

    public VirtualNode(int hash, String physicalNodeId) {
        this(hash);
        this.physicalNodeId = physicalNodeId;
    }

    public int getHash() {
        return hash;
    }

    public void setHash(int hash) {
        this.hash = hash;
    }

    public String getPhysicalNodeId() {
        return physicalNodeId;
    }

    public void setPhysicalNodeId(String physicalNodeId) {
        this.physicalNodeId = physicalNodeId;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int compareTo(VirtualNode o) {
        return Integer.compare(this.hash, o.getHash());
    }
    
	public String serialize() {
		return this.toJSON().toString();
	}

	public JsonObject toJSON() {
		JsonObject jsonObj = Json.createObjectBuilder()
				.add("hash",this.hash)
				.add("index",this.index)
				.add("physicalNodeId",this.physicalNodeId)
				.build();
		return jsonObj;
	}
}
