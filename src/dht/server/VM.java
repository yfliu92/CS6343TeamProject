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
	
	public int getId() {
		return this.id;
	}
	
	public Range getRange() {
		return hashRange;
	}
	
	public void setRangeStart(int rangeStart) {
		this.hashRange.rangeStart = rangeStart;
	}
	
	public void setRangeEnd(int rangeEnd) {
		this.hashRange.rangeEnd = rangeEnd;
	}
}
