package dht.common;
import java.util.List;

import dht.server.*;

public class RingRange extends Range {
	public RingRange(){
		super();
	}
	
	public static int getVMIdFromHashVal(List<VM> vmlist, int hashVal) {
		int vmId = -1;
		
		for(VM vm: vmlist) {
			if (vm.getRange().rangeStart < hashVal && hashVal <= vm.getRange().rangeEnd) {
				vmId = vm.getId();
				break;
			}
		}
		
		return vmId;
	}
	
	public static Range getNewRange(List<VM> activeVMs, Proxy proxy, int nodeHash) {
		int existingVMId = RingRange.getVMIdFromHashVal(activeVMs, nodeHash);
		
		// only for ring implementation
		// similar methods could be implemented for other DHT implementation
		VM nextVM = proxy.findNodeById(existingVMId);
		
		int newVMId = activeVMs.size() + 1;
		int newRangeStart = nextVM.getRange().rangeStart;
		int newRangeEnd = nodeHash;
		
		
		
		Range newRange = new Range(newRangeStart, newRangeEnd, newVMId);
		
		return newRange;
	}
	
	public static void updateOtherVMs(List<VM> activeVMs, Proxy proxy, int nodeHash) {
		int existingVMId = RingRange.getVMIdFromHashVal(activeVMs, nodeHash);
		VM nextVM = proxy.findNodeById(existingVMId);
		int nextRangeStart = nodeHash + 1;
		nextVM.setRangeStart(nextRangeStart);
	}
}
