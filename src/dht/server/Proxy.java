package dht.server;
import java.util.*;
import dht.common.*;

public class Proxy extends VM {
	List<VM> activeVMs;
	int activeno;
	
	public Proxy(){
		super();
		activeVMs = new LinkedList<VM>();
		activeno = 0;
	}
	
	public void printConfig() {
		System.out.println("System Information: ");
		System.out.println("Total number of VMs: " + activeVMs.size());
		
		for(VM vm: activeVMs) {
			System.out.println(vm.vmId + " load " + vm.currentLoad);
		}
	}
	
	public VM findNodeById(int vmId) {
		VM node = null;
		for(VM vm: activeVMs) {
			if (vm.id == vmId) {
				node = vm;
				break;
			}
		}
		
		return node;
	}
	
	public VM findNodeByIP(String IP) {
		VM node = null;
		for(VM vm: activeVMs) {
			if (vm.IP.equals(IP)) {
				node = vm;
				break;
			}
		}
		
		return node;
	}
	
	public void addNode(String IP) {
		int nodeHash = Hashing.getHashValFromIP(IP);
		// get the range of its next node range id

		// only for ring implementation
		// similar methods could be implemented for other DHT implementation
		
		Range newRange = RingRange.getNewRange(activeVMs, (Proxy)this, nodeHash);
		
		VM vm = new VM();
		
		vm.id = activeVMs.size() + 1;
		vm.IP = IP;
		vm.neighbors = new LinkedList<VM>();
		vm.neighbors.addAll(activeVMs);  // for distributed version
		vm.proxy = (Proxy)this; // for centralized version
		vm.hashRange = newRange;
		
		activeVMs.add(vm);
		
		// all other nodes should update
		// new range should be allocated
		// range of its other nodes should be updated, depending on which DHT method used.
		// in ring implementation, the next node should be updated in its range
		
		RingRange.updateOtherVMs(activeVMs, (Proxy)this, nodeHash);
		
		
		activeno++;
	}
	
	public void removeNode(String IP) {
		VM vm = findNodeByIP(IP);
		int nodeHash = Hashing.getHashValFromIP(vm.IP);
		
		activeno--;
	}
	
	public void loadBalance() {
		
	}

}
