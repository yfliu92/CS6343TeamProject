package dht.common;

public class Hashing {
	
	// maximum hash value configurable from config file
	public static int maxNum = 10000; 
	
	public static int getHashValFromIP(String IP) {
		String md5IP = IP;
		return getHashValFromKeyword(md5IP);
	}
	
	public static int getHashValFromKeyword(String keyword) {
		int hashVal = keyword.hashCode();
		hashVal = hashVal % maxNum;
		
		return hashVal;
	}
	
	public static int getVMIdFromHashVal(int hashVal, int numVMs) {
		int vmID = 0;
		
		vmID = hashVal % numVMs;
		
		return vmID;
	}
	
	public static int getVMIdFromKeyword(String keyword, int numVMs) {
		return getVMIdFromHashVal(getHashValFromKeyword(keyword), numVMs);
	}
	
	
}
