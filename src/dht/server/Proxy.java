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
	
	public VM findNode(String IP) {
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
		int existingVMId = Range.getVMIdFromHashVal(nodeHash);
		
		// only for ring implementation
		// similar methods could be implemented for other DHT implementation
		VM nextVM = findNodeById(existingVMId);
		
		int newVMId = activeno + 1;
		int newRangeStart = nextVM.hashRange.rangeStart;
		int newRangeEnd = nodeHash;
		
		int nextRangeStart = nodeHash + 1;
		
		Range newRange = new Range(newRangeStart, newRangeEnd, newVMId);
		
		VM vm = new VM();
		
		vm.id = newVMId;
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
		
		nextVM.hashRange.rangeStart = nextRangeStart;
		
		
		activeno++;
	}
	
	public void removeNode(String IP) {
		VM vm = findNode(IP);
		int nodeHash = Hashing.getHashValFromIP(vm.IP);
		
		activeno--;
	}
	
	public void loadBalance() {
		
	}

}
