package com.google.code.gossip.manager.Ring;
import java.util.*;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import com.google.code.gossip.manager.Ring.Datum;
import com.google.code.gossip.manager.Ring.Hashing;
//import dht.common.response.*;
import com.google.code.gossip.manager.Ring.Response;

public class DataStore {
	
	HashMap<String, Datum> dataStore;
	
	public DataStore() {
		dataStore = new HashMap<String, Datum>();
	}
	
	public Datum read(String datum) {
		Datum result = dataStore.get(datum);
		return result;
	}
	
	public String readRes(String datum) {
		Datum result = dataStore.get(datum);
		if (result == null) {
			Response response = new Response(false, "Data not found");
			return response.serialize();
		}
		else {
			DataResponse response = new DataResponse(true, "", result);
			return response.serialize();
		}
	}
	
	public Datum write(String datumStr, int hash, int[] virtualnodes) {
		Datum datum = dataStore.get(datumStr);
		if (datum == null) {
			Datum newDatum = new Datum(datumStr, hash, virtualnodes);
			dataStore.put(datumStr, newDatum);
			datum = newDatum;
		}
		else {
			for(int virtualnode: virtualnodes) {
				datum.addVirtualNode(virtualnode);
			}
		}
		
		return datum;
	}
	
	public String writeRes(String datumStr, int hash, int[] virtualnode) {
		Datum result = write(datumStr, hash, virtualnode);
		DataResponse response = new DataResponse(true, "latest epoch: " + result.getEpoch(), result);
		return response.serialize();
	}
	
	public String writeRandomRes(int batchsize, PhysicalNode physicalNode) {
		int num = 0;
		while(num < batchsize) {
			String datumStr = Hashing.getRanStr(0);
			int rawhash = Hashing.getHashValFromKeyword(datumStr);
			int virtualnode = physicalNode.getLookupTable().getTable().getVirtualNode(datumStr).getHash();
			
			List<VirtualNode> virtualnodes = physicalNode.getLookupTable().getTable().getSuccessors(datumStr);
			int[] virtualnodeids = new int[1 + virtualnodes.size()];
			virtualnodeids[0] = virtualnode;
			for(int i = 0; i < virtualnodes.size(); i++) {
				virtualnodeids[i + 1] = virtualnodes.get(i).getHash();
			}
			
			write(datumStr, rawhash, new int[] {virtualnode});
			num++;
		}
		
		Response response = new Response(true, "A total of " + num + " data records are written");
		return response.serialize();
	}
	
	public String updateRandomRes(int updateNum, PhysicalNode physicalNode) {
		int size = this.dataStore.size();
		Object[] keys = this.dataStore.keySet().toArray();
		Random ran = new Random();
		int num = 0;
		while(num < updateNum) {
			
			String datumStr = String.valueOf(keys[ran.nextInt(size)]);
			int rawhash = Hashing.getHashValFromKeyword(datumStr);
			int virtualnode = physicalNode.getLookupTable().getTable().getVirtualNode(datumStr).getHash();
			write(datumStr, rawhash, new int[] {virtualnode});
			num++;
		}
		
		Response response = new Response(true, "A total of " + updateNum + " data records are updated");
		return response.serialize();
	}
	
	public String listData() {
		StringBuilder result = new StringBuilder();
		if (dataStore == null || dataStore.size() == 0) {
			result.append("No data found\n");
		}
		else {
			for(HashMap.Entry<String, Datum> entry: dataStore.entrySet()) {
				result.append(entry.getValue().toString());
				result.append("\n");
			}
		}
		
		return result.toString();
	}
	
	public String listDataRes() {
		if (dataStore == null || dataStore.size() == 0)
		{
			String message = "No data found";
			Response response = new Response(false, message);
			return response.serialize();
		}
		else {
			String message = "A total of " + dataStore.size() + " records are found";
			JsonObjectBuilder jsonBuilder = Json.createObjectBuilder();
			int i = 0;
			for(HashMap.Entry<String, Datum> entry: dataStore.entrySet()) {
				jsonBuilder.add(String.valueOf(i), entry.getValue().toString());
				i++;
			}
			DataResponse response = new DataResponse(true, message, jsonBuilder.build());
			return response.serialize();
		}
	}

}
