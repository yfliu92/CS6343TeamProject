package dht.server;
import java.util.*;
import dht.common.*;

public class VM {
	int id;
	String vmId;
	String IP;
	String currentLoad;
	List<VM> neighbors;
	Proxy proxy;
	Range hashRange;
	
	public static int getVMIdFromHashVal(List<VM> vmlist, int hashVal) {
		int vmId = -1;
		
		for(VM vm: vmlist) {
			if (vm.hashRange.rangeStart < hashVal && hashVal <= vm.hashRange.rangeEnd) {
				vmId = vm.id;
				break;
			}
		}
		
		return vmId;
	}
}
