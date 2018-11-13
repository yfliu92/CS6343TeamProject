package com.google.code.gossip.manager.Ring;

import java.util.*;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

public class Datum {

	String datum;
	int epoch;
	String timestamp;
	int rawhash;
//	int virtualnode;
	List<Integer> virtualNodes;
	
//	public Datum(String datum, int hash, int virtualnode) {
//		this.datum = datum;
//		this.epoch = 1;
//		this.timestamp = String.valueOf(new Date().getTime());
//		this.rawhash = hash;
//		this.virtualnode = virtualnode;
//	}
//	
//	public void updateVirtualNode(int virtualnode) {
//		this.epoch++;
//		this.virtualnode = virtualnode;
//		this.timestamp = String.valueOf(new Date().getTime());
//	}
	
	public Datum(String datum, int hash, int[] virtualnodes) {
		this.datum = datum;
		this.epoch = 1;
		this.timestamp = String.valueOf(new Date().getTime());
		this.rawhash = hash;
		virtualNodes = new LinkedList<Integer>();
		for(int virtualnode: virtualnodes) {
			virtualNodes.add(virtualnode);
		}
	}
	
	public void addVirtualNode(int virtualnode) {
		int index = this.virtualNodes.indexOf(virtualnode);
		if (index < 0) {
			this.virtualNodes.add(virtualnode);
		}
		this.epoch++;
		this.timestamp = String.valueOf(new Date().getTime());
	}
	
	public void removeVirtualNode(int virtualnode) {
		int index = this.virtualNodes.indexOf(virtualnode);
		if (index >= 0) {
			this.virtualNodes.remove(index);
		}
		this.epoch++;
		this.timestamp = String.valueOf(new Date().getTime());
	}
	
	public void moveVirtualNode(int oldnode, int newnode) {
		int index1 = this.virtualNodes.indexOf(oldnode);
		if (index1 >= 0) {
			this.virtualNodes.remove(index1);
		}
		int index2 = this.virtualNodes.indexOf(newnode);
		if (index2 < 0) {
			this.virtualNodes.add(newnode);
		}
		this.epoch++;
		this.timestamp = String.valueOf(new Date().getTime());
	}
	
	
	public int getEpoch() {
		return this.epoch;
	}
	
	public int getHash() {
		return this.rawhash;
	}
	
//	public int getVirtualNode() {
//		return this.virtualnode;
//	}
	
	public List<Integer> getVirtualNodes() {
		return this.virtualNodes;
	}
	
	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append(this.datum);
		result.append(" (");
		result.append("hash: " + this.rawhash);
		result.append(", ");
		result.append("virtual nodes: " + Arrays.toString(this.virtualNodes.toArray()));
		result.append(", ");
		result.append("epoch: " + this.epoch);
		result.append(") ");
		return result.toString();
	}
	
	public String serialize() {
		return this.toJSON().toString();
	}
	
	public JsonObject toJSON() {
		JsonObject jsonObj = Json.createObjectBuilder()
				.add("datum",this.datum)
				.add("epoch",this.epoch)
				.add("timestamp",this.timestamp)
				.add("hash",this.rawhash)
				.add("virtualnodes",Arrays.toString(this.virtualNodes.toArray()))
				.build();
		return jsonObj;
	}
}
