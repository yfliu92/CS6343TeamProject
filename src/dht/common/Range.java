package dht.common;

// hash value range allocated to a vm
public class Range {
	public int rangeStart;
	public int rangeEnd;
	int vmId;
	
	public Range() {
		
	}
	
	public Range(int rangeStart, int rangeEnd, int vmId){
		this.rangeStart = rangeStart;
		this.rangeEnd = rangeEnd;
		this.vmId = vmId;
	}
}
