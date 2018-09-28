package dht.server;
import java.util.*;
import dht.common.Range;

public class VM {
	int id;
	String vmId;
	String IP;
	String currentLoad;
	List<VM> neighbors;
	Proxy proxy;
	Range hashRange;
}
