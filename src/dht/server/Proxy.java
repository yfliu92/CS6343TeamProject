package dht.server;
import java.util.*;

import dht.common.Hashing;
import dht.common.Range;

public class Proxy extends VM {
	List<VM> activeVMs;
	int activeno;
	
	public Proxy(){
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
	
	public void addNode(String IP) {
		int nodeHash = Hashing.getHashValFromIP(IP);
		// get the range of its next node range id
		int rangeId = Range.getRangeIdFromHashVal(nodeHash);
		
		VM vm = new VM();
		vm.id = activeno + 1;
		vm.IP = IP;
		vm.neighbors = new LinkedList<VM>();
		vm.neighbors.addAll(activeVMs);
		vm.proxy = (Proxy)this;
		activeVMs.add(vm);
		
		// all other nodes should update
		// new range should be allocated
		// range of its next node should be updated
		
		activeno++;
	}
	
	public void removeNode(VM vm) {
		int nodeHash = Hashing.getHashValFromIP(vm.IP);
		
		activeno--;
	}
	
	public void loadBalance() {
		
	}

}
