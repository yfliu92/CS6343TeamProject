package dht.server;
import java.util.*;
import dht.common.*;

public class VM {
	int id;
	String vmId;
	String IP;
	String currentLoad;
	// in centralized version, it represents all VMs stored in proxy
	// in distributed version, it represents peers
	List<VM> neighbors;  
	Proxy proxy;
	Range hashRange;
	
	public int getId() {
		return this.id;
	}
	
	public Range getRange() {
		return hashRange;
	}
	
	public void setRange(Range range) {
		this.hashRange = range;
	}
	
	// receive remote request from proxy or peers to add its neighbors
	public void AddNeighbor(VM neighbor) {
		this.neighbors.add(neighbor);
	}
	
	// receive remote request from proxy or peers to remove its neighbors
	public void RemoveNeighbor(int vmId) {
		for(int i = 0; i < this.neighbors.size(); i++) {
			if (this.neighbors.get(i).id == vmId) {
				this.neighbors.remove(i);
				break;
			}
		}
	}
	
	// request proxy to add node
	public void requestAddNode(String IP) {
		// send request to call addNode(IP) on the proxy
	}
	
	// request proxy to add the current node
	public void requestAddNode() {
		String IP = ""; // current IP
		requestAddNode(IP);
	}
	
	// request proxy to remove node
	public void requestRemoveNode(String IP) {
		// send request to call addNode(IP) on the proxy
	}
	
	// request proxy to add the current node
	public void requestRemoveNode() {
		String IP = ""; // current IP
		requestRemoveNode(IP);
	}
}
