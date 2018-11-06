package dht.common;
import java.util.*;

public class Hashing {
	
	// maximum hash value configurable from config file
	public static final int MAX_HASH = 10000; 
	
	public static int getHashValFromIP(String IP) {
		String md5IP = IP;
		return getHashValFromKeyword(md5IP);
	}
	
	public static int getHashValFromKeyword(String keyword) {
		int hashVal = keyword.hashCode();
		hashVal = hashVal % MAX_HASH;
		
		return Math.abs(hashVal);
	}
	
	public static int getVMIdFromHashVal(int hashVal, int numVMs) {
		int vmID = 0;
		
		vmID = hashVal % numVMs;
		
		return vmID;
	}
	
	public static int getVMIdFromKeyword(String keyword, int numVMs) {
		return getVMIdFromHashVal(getHashValFromKeyword(keyword), numVMs);
	}
	
	public static String getRanStr(int maxlength) {
		Random ran = new Random();
		if (maxlength == 0) {
			maxlength = ran.nextInt(10) + 1;
		}
		
		StringBuilder result = new StringBuilder();
		for(int i = 0; i < maxlength; i++) {
			result.append((char)(ran.nextInt(26)+97));
		}
		return result.toString();
	}
	
	public static void main(String[] args) {
		System.out.println("Hash value of ata:" + "ata".hashCode()%10000);
		System.out.println("Hash value of atb:" + "atb".hashCode()%10000);
		System.out.println("Hash value of atc:" + "atc".hashCode()%10000);
		System.out.println("Hash value of atd:" + "atd".hashCode()%10000);
		System.out.println("Hash value of ate:" + "ate".hashCode()%10000);
		System.out.println("Hash value of aeraata:" + "aeraata".hashCode()%10000);
		System.out.println("Hash value of aeraatb:" + "aeraatb".hashCode()%10000);
		System.out.println("Hash value of aeraatc:" + "aeraatc".hashCode()%10000);
		System.out.println("Hash value of aeraatd:" + "aeraatd".hashCode()%10000);
		System.out.println("Hash value of aeraate:" + "aeraate".hashCode()%10000);
		System.out.println("Hash value of aeraataaeraata:" + "aeraataaeraata".hashCode()%10000);
		System.out.println("Hash value of aeraataaeraatb:" + "aeraataaeraatb".hashCode()%10000);
		System.out.println("Hash value of aeraataaeraatc:" + "aeraataaeraatc".hashCode()%10000);
		System.out.println("Hash value of aeraataaeraatd:" + "aeraataaeraatd".hashCode()%10000);
		System.out.println("Hash value of aeraataaeraate:" + "aeraataaeraate".hashCode()%10000);
		System.out.println("Hash value of aeraataaeraataaeraata:" + "aeraataaeraataaeraata".hashCode()%10000);
		System.out.println("Hash value of aeraataaeraataaeraatb:" + "aeraataaeraataaeraatb".hashCode()%10000);
		System.out.println("Hash value of aeraataaeraataaeraatc:" + "aeraataaeraataaeraatc".hashCode()%10000);
		System.out.println("Hash value of aeraataaeraataaeraatd:" + "aeraataaeraataaeraatd".hashCode()%10000);
		System.out.println("Hash value of aeraataaeraataaeraate:" + "aeraataaeraataaeraate".hashCode()%10000);
	}
	
}
