package dht.rush.clusters;

import java.util.*;
import java.util.Map.Entry;

import javax.json.*;

public class Cluster {
    private String id;  // ip:port
    private String ip;
    private String port;
    private int numberOfChildren;
    private double weight;
    private Boolean isActive;
    private String parentId;
    private List<Cluster> subClusters;
    private Map<String, Integer> placementGroupMap;

    private ClusterStructureMap cachedTreeStructure;
    private ClusterType type;

    public Cluster() {
        this.cachedTreeStructure = new ClusterStructureMap();
        this.subClusters = new ArrayList<>();
        this.placementGroupMap = new HashMap<>();
        this.type = ClusterType.NONE_TYPE;
    }

    public Cluster(String id, String ip, String port, String parentId, int numberOfChildren, double weight, Boolean isActive) {
        this.id = id;
        this.ip = ip;
        this.port = port;
        this.parentId = parentId;
        this.numberOfChildren = numberOfChildren;
        this.weight = weight;
        this.isActive = isActive;
        this.cachedTreeStructure = new ClusterStructureMap();
        this.subClusters = new ArrayList<>();
        this.placementGroupMap = new HashMap<>();
        this.type = ClusterType.NONE_TYPE;
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

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public int getNumberOfChildren() {
        return numberOfChildren;
    }

    public void setNumberOfChildren(int numberOfChildren) {
        this.numberOfChildren = numberOfChildren;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public Boolean getActive() {
        return isActive;
    }

    public void setActive(Boolean active) {
        isActive = active;
    }

    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    public ClusterStructureMap getCachedTreeStructure() {
        return cachedTreeStructure;
    }

    public void setCachedTreeStructure(ClusterStructureMap cachedTreeStructure) {
        this.cachedTreeStructure = cachedTreeStructure;
    }

    public List<Cluster> getSubClusters() {
        return subClusters;
    }

    public void setSubClusters(List<Cluster> subClusters) {
        this.subClusters = subClusters;
    }

    @Override
    public String toString() {
        return "Cluster{" +
                "id='" + id + '\'' +
                ", ip='" + ip + '\'' +
                ", port='" + port + '\'' +
                ", weight=" + weight +
                ", isActive=" + isActive +
                ", parentId='" + parentId + '\'' +
                '}';
    }

    public Map<String, Integer> getPlacementGroupMap() {
        return placementGroupMap;
    }

    public void setPlacementGroupMap(Map<String, Integer> placementGroupMap) {
        this.placementGroupMap = placementGroupMap;
    }

    public ClusterType getType() {
        return type;
    }

    public String getPlacementGroupString() {
        StringBuilder sb = new StringBuilder();
        if (this.getPlacementGroupMap().size() > 0) {
            for (Map.Entry<String, Integer> entry : this.getPlacementGroupMap().entrySet()) {
                String pgid = entry.getKey();
                int r = entry.getValue();
                sb.append(pgid + ":" + r + ",");
            }

            if (sb.lastIndexOf(",") > -1) {
                sb.deleteCharAt(sb.lastIndexOf(","));
            }
        }
        return sb.toString();
    }

    /**
     * The order of sub clusters in the list matters
     *
     * @return
     */
    public String getSubClusterListString() {
        StringBuilder sb = new StringBuilder();
        if (this.getSubClusters().size() > 0) {
            for (Cluster c : this.getSubClusters()) {
                sb.append(c.getId() + ",");
            }
            if (sb.lastIndexOf(",") > -1) {
                sb.deleteCharAt(sb.lastIndexOf(","));
            }
        }
        return sb.toString();
    }
    
	public String serialize() {
		return this.toJSON().toString();
	}

	public JsonObject toJSON() {
		JsonObjectBuilder jsonBuilder = Json.createObjectBuilder();
		jsonBuilder.add("id",this.id)
			.add("ip",this.ip)
			.add("port",this.port)
			.add("weight",this.weight)
			.add("isActive",this.isActive)
			.add("parentId",this.parentId);
		JsonObjectBuilder placementGroupJson = Json.createObjectBuilder();
		for(Map.Entry<String, Integer> group: placementGroupMap.entrySet()) {
			placementGroupJson.add(group.getKey(), group.getValue());
		}
		if (this.subClusters.size() > 0) {
			JsonArrayBuilder subclusters = Json.createArrayBuilder();
			for(int i = 0; i < this.subClusters.size(); i++) {
				subclusters.add(this.subClusters.get(i).toJSON());
			}
			jsonBuilder.add("subClusters", subclusters.build());
		}

		jsonBuilder.add("placementGroupMap",placementGroupJson.build());
		jsonBuilder.add("cachedTreeStructure", this.cachedTreeStructure.toJSON());
		jsonBuilder.add("type", this.type.toString());
		
		return jsonBuilder.build();
	}
	
	public static Cluster fromJSON(JsonObject data) {
		Cluster cluster = new Cluster();
		if (data.containsKey("id")) {
			cluster.id = data.getString("id");
		}
		if (data.containsKey("ip")) {
			cluster.ip = data.getString("ip");
		}
		if (data.containsKey("port")) {
			cluster.port = data.getString("port");
		}
		if (data.containsKey("parentId")) {
			cluster.parentId = data.getString("parentId");
		}
		if (data.containsKey("numberOfChildren")) {
			cluster.numberOfChildren = data.getInt("numberOfChildren");
		}
		if (data.containsKey("weight")) {
			cluster.weight = Double.valueOf(data.get("weight").toString());
		}
		if (data.containsKey("isActive")) {
			cluster.isActive = data.getBoolean("isActive");
		}
		if (data.containsKey("type")) {
			cluster.type = ClusterType.valueOf(data.getString("type"));
		}
		cluster.cachedTreeStructure = new ClusterStructureMap();
		cluster.subClusters = new ArrayList<>();
		cluster.placementGroupMap = new HashMap<>();
        if (data.containsKey("placementGroupMap")) {
        	JsonObject groups = data.getJsonObject("placementGroupMap");
        	for(Entry<String, JsonValue> group: groups.entrySet()) {
        		cluster.placementGroupMap.put(group.getKey(), Integer.valueOf(group.getValue().toString()));
        	}
        }
        if (data.containsKey("subClusters")) {
        	JsonArray subclustersJson = data.get("subClusters").asJsonArray();
        	for(int i = 0; i < subclustersJson.size(); i++) {
        		JsonObject clusterJson = subclustersJson.get(i).asJsonObject();
        		cluster.subClusters.add(fromJSON(clusterJson));
        	}
        }
        
        return cluster;
	}
	
	public void print() {
		System.out.print("\n");
		System.out.print("id: " + this.id + ", ");
		System.out.print("ip: " + this.ip + ", ");
		System.out.print("port: " + this.port + ", ");
		System.out.print("weight: " + this.weight + ", ");
		System.out.print("isActive: " + this.isActive + ", ");
		System.out.print("parentId: " + this.parentId + ", ");
		System.out.print("subClusters: " + this.getSubClusterListString());
		StringBuilder placementgroup = new StringBuilder();
		for(Map.Entry<String, Integer> group: placementGroupMap.entrySet()) {
			if (placementgroup.length() > 0) {
				placementgroup.append(" ");
			}
			placementgroup.append("<" + group.getKey() + ", " + group.getValue() + ">");
		}
		System.out.print("placementGroupMap: " + placementgroup + ", ");
		// cachedTreeStructure
		System.out.print("ClusterType: " + this.type.toString());
		System.out.print("\n");
		
	}
}
