package server;
import java.util.*;

public class Proxy extends VM {
	List<VM> activeVMs;
	
	public Proxy(){
		activeVMs = new LinkedList<VM>();
	}
	
	public void printConfig() {
		System.out.println("System Information: ");
		System.out.println("Total number of VMs: " + activeVMs.size());
		
		for(VM vm: activeVMs) {
			System.out.println(vm.vmId + " load " + vm.currentLoad);
		}
	}
	
	public void addNode() {
		
	}
	
	public void removeNode() {
		
	}
}
