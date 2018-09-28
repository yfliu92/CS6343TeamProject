package dht.common;

// hash value range allocated to a vm
public class Range {
	int rangeStart;
	int rangeEnd;
	int rangeId;
	
	public Range() {
		
	}
	
	public Range(int rangeStart, int rangeEnd, int rangeId){
		this.rangeStart = rangeStart;
		this.rangeEnd = rangeEnd;
		this.rangeId = rangeId;
	}
	
	public static int getRangeIdFromHashVal(int hashVal) {
		int rangeId = 0;
		return rangeId;
	}
}
