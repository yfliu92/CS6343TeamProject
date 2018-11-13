package com.google.code.gossip.manager.Ring;

import com.google.code.gossip.manager.Ring.Hashing;
import com.google.code.gossip.manager.Ring.control_client;

import java.io.BufferedReader;
import java.io.Console;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.sql.Timestamp;
import java.util.*;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

public class PhysicalNode {

    private String id;

    private String address;

    private int port;

    private String status;

    private LookupTable lookupTable;

    private List<VirtualNode> virtualNodes;

    public final static String STATUS_ACTIVE = "active";

    public final static String STATUS_INACTIVE = "inactive";

    public PhysicalNode() {
    	this.lookupTable = new LookupTable();
    	this.virtualNodes = new ArrayList<>();
    }

    public PhysicalNode(String ID, String ip, int port, String status){
        this.id = ID;
        this.address = ip;
        this.port = port;
        this.status = status;
    }

    public PhysicalNode(String ID, String ip, int port, String status, List<VirtualNode> nodes){
        this(ID, ip, port, status);
        this.virtualNodes = nodes;
        this.lookupTable = new LookupTable();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public LookupTable getLookupTable() {
        return lookupTable;
    }

    public void setLookupTable(LookupTable lookupTable) {
        this.lookupTable = lookupTable;
    }

    public List<VirtualNode> getVirtualNodes() {
        return virtualNodes;
    }

    public void setVirtualNodes(List<VirtualNode> virtualNodes) {
        this.virtualNodes = virtualNodes;
    }
    
    public String listNodes() {
//    	BinarySearchList list = lookupTable.getTable();
//    	StringBuilder result = new StringBuilder(); 
//    	result.append("Existing nodes (" + list.size() + "): ");
//    	
//    	for(int i = 0; i < list.size(); i++) {
//    		result.append("   Virtual Node" + list.get(i).getHash() + ", Physical Node: " + list.get(i).getPhysicalNodeId());
//    	}
//    	
//    	return result.toString();
    	
    	return lookupTable.serialize();
    }
    
    public String addNode(String ip, int port) {
    	int hash = lookupTable.getTable().getRanHash();

    	String result = "";
    	if (hash >= 0) {

    		try {
        		addNode(ip, port, hash);
        		result = "true|Node added successfully, hash " + hash;
    		}
    		catch(Exception ee) {
    			result = "false|exception when adding node (with hash " + hash + ") - " + ee.toString();
    		}

    	}
    	else {
    		result = "false|Virtual node exhausted";
    	}

    	return result;
    }

    public String dataTransfer(VirtualNode fromNode, VirtualNode toNode, int start, int end){
        StringBuilder result = new StringBuilder();
        // Get the two virtual nodes' physical node ID
        String address1 = " (" + fromNode.getPhysicalNodeId() + ")";
        String address2 = " (" + toNode.getPhysicalNodeId() + ")";
        // v means virtual node
//        result.append("\r\nfrom v" + fromNode.getHash() + address1 +  " to v" + toNode.getHash() + address2 + ":\r\n"
//                + "Transferring data for hash range of (" + start + ", " + end + ")");
        result.append("\nfrom v" + fromNode.getHash() + address1 +  " to v" + toNode.getHash() + address2 + ": "
                + "Transferring data for hash range of (" + start + ", " + end + ")");
        
        return result.toString();
    }



    // Add a physical node that maps to 10 virtual node (configurable in the configuration file)
    public String addNode(String ip, int port, int[] hashes){
        String result = "";
        for (int hash : hashes){
            result += addNode(ip, port, hash);
        }
        return result;
    }
    //Add a physical node that maps to just 1 virtual node
    public String addNode(String ip, int port, int hash){
        String result = "";
        // Create an id for the new physical node
        String physicalNodeID =  ip + "-" + Integer.toString(port);
        // Create a new virtual node that maps to this physical node
        // Assume just 1 virtual node maps to this physical node
        VirtualNode vNode = new VirtualNode(hash, physicalNodeID);
        // Put the virtual node on the ring
        try {
            if (lookupTable.getTable().add(vNode) == false){
                result = "\nfalse|virtual node " + hash +" already exists";
            }
            else {
                result = "\ntrue|Virtual Node added successfully at " + hash;
                // Get the index of the inserted virtual node in the BinarySearchList
                int index = vNode.getIndex();
                // Get its successors and predecessors
                List<VirtualNode> successors = new ArrayList<>();
                List<VirtualNode> predecessors = new ArrayList<>();
                for (int i = 0; i < ProxyServer.numOfReplicas; i++){
                    VirtualNode next = lookupTable.getTable().next(index + i);
                    successors.add(next);
                    VirtualNode pre = lookupTable.getTable().pre(index - i);
                    predecessors.add(pre);
                }

                // Check if this physical node already exits in the physicalNodeMap
                // If not, add it in the physicalNodeMap
                if (!lookupTable.getPhysicalNodeMap().containsKey(physicalNodeID)) {
                    List<VirtualNode> list = new ArrayList<>();
                    list.add(vNode);
                    PhysicalNode physicalNode = new PhysicalNode(physicalNodeID, ip, port, STATUS_ACTIVE, list);
                    lookupTable.getPhysicalNodeMap().put(physicalNodeID, physicalNode);
                } else {
                    lookupTable.getPhysicalNodeMap().get(physicalNodeID).getVirtualNodes().add(vNode);
                }

                for (int i = 0; i < successors.size(); i++){
                    if (i != successors.size() - 1)
                        result += dataTransfer(successors.get(i), vNode, predecessors.get(successors.size() - 1 - i).getHash() + 1, predecessors.get(successors.size() - 2 - i).getHash());
                    else
                        result += dataTransfer(successors.get(i), vNode, predecessors.get(successors.size() - 1 - i).getHash() + 1, hash);
                }
                
                lookupTable.getTable().updateIndex();

                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                lookupTable.setEpoch(timestamp.getTime());
            }
        }
        catch(Exception ee) {
            result = "false|exception when adding node (with hash " + hash + ") - " + ee.toString();
        }

        return result;
    }


    // Delete virtual node by its hash value
    public String deleteNode(int hash) {
        VirtualNode vNode = new VirtualNode(hash);
        int index = Collections.binarySearch(lookupTable.getTable(), vNode);
        if (index < 0){
           return "\nfalse|" + "hash " + hash + " is not a virtual node.";
        }
        // Get its successors and predecessors
        List<VirtualNode> successors = new ArrayList<>();
        List<VirtualNode> predecessors = new ArrayList<>();
        for (int i = 0; i < ProxyServer.numOfReplicas; i++){
            VirtualNode next = lookupTable.getTable().next(index + i);
            successors.add(next);
            VirtualNode pre = lookupTable.getTable().pre(index - i);
            predecessors.add(pre);
        }

        // Delete the virtual node from the ring of virtual nodes
        VirtualNode virtualNodeToDelete = lookupTable.getTable().remove(index);

        String result = "\ntrue|virtual node " + hash + " removed successfully";
        for (int i = 0; i < successors.size(); i++){
            if (i != successors.size() - 1)
                result += dataTransfer(predecessors.get(predecessors.size() - 2 - i), successors.get(i), predecessors.get(predecessors.size() - 1 - i).getHash() + 1, predecessors.get(successors.size() - 2 - i).getHash());
            else
                result += dataTransfer(successors.get(0), successors.get(i), predecessors.get(predecessors.size() - 1 - i).getHash() + 1, hash);
        }

        // Remove the virtual node from its physical node's virtual node list
        List<VirtualNode> list = lookupTable.getPhysicalNodeMap().get(virtualNodeToDelete.getPhysicalNodeId()).getVirtualNodes();
        int idx = Collections.binarySearch(list, virtualNodeToDelete);
        lookupTable.getPhysicalNodeMap().get(virtualNodeToDelete.getPhysicalNodeId()).getVirtualNodes().remove(idx);

        // Update the local timestamp
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        lookupTable.setEpoch(timestamp.getTime());
        
        return result;
    }

    //// Delete virtual node by its hash value
    public String deleteNode(VirtualNode node){
        int index = Collections.binarySearch(lookupTable.getTable(), node);
        if (index < 0){
            return "\nfalse|" + "hash " + node.getHash() + " is not a virtual node.";
        }

        // Get its successors and predecessors
        List<VirtualNode> successors = new ArrayList<>();
        List<VirtualNode> predecessors = new ArrayList<>();
        for (int i = 0; i < ProxyServer.numOfReplicas; i++){
            VirtualNode next = lookupTable.getTable().next(index + i);
            successors.add(next);
            VirtualNode pre = lookupTable.getTable().pre(index - i);
            predecessors.add(pre);
        }

        // Delete the virtual node from the ring of virtual nodes
        VirtualNode virtualNodeToDelete = lookupTable.getTable().remove(index);
        String result = "\ntrue|virtual node " + node.getHash() + " removed successfully";
        for (int i = 0; i < successors.size(); i++){
            if (i != successors.size() - 1)
                result += dataTransfer(predecessors.get(predecessors.size() - 2 - i), successors.get(i), predecessors.get(predecessors.size() - 1 - i).getHash() + 1, predecessors.get(successors.size() - 2 - i).getHash());
            else
                result += dataTransfer(successors.get(0), successors.get(i), predecessors.get(predecessors.size() - 1 - i).getHash() + 1, node.getHash());
        }

        // Update the local timestamp
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        lookupTable.setEpoch(timestamp.getTime());
        return result;
    }

    public String failNode(String ip, int port){
        String physicalNodeID = ip + "-" + Integer.toString(port);
        lookupTable.getPhysicalNodeMap().get(physicalNodeID).setStatus("inactive");
        List<VirtualNode> virtualNodes = lookupTable.getPhysicalNodeMap().get(physicalNodeID).getVirtualNodes();
        if (virtualNodes.size() == 0){
            return "This physical node has no virtual nodes";
        }
        String result = "\ntrue|Node " + physicalNodeID + " removed successfully: ";
        for (VirtualNode node : virtualNodes ){
            result += deleteNode(node);
        }
        // Set the virtual node list of the failed node to be empty
        lookupTable.getPhysicalNodeMap().get(physicalNodeID).setVirtualNodes(new ArrayList<VirtualNode>());
        return result;
    }

    // Change the position of a virtual node on the ring to balance load
    public String loadBalance(int delta, int hash) { // move the node clockwise if delta > 0, counterclockwise if delta < 0
        String result = "";
        VirtualNode node = new VirtualNode(hash);
        int index = Collections.binarySearch(lookupTable.getTable(), node);
        node = lookupTable.getTable().get(index);
        String physicalNodeID = node.getPhysicalNodeId();
        if (index < 0) {
            return "\nfalse|" + "hash " + hash + " is not a virtual node.";
        }
        int newHash = hash + delta;
        VirtualNode newNode = new VirtualNode(newHash,physicalNodeID);
        if (lookupTable.getTable().add(newNode) == false) {
            result = "\nfalse|virtual node " + newHash + " already exists";
        }
        else {
            result = "\ntrue|virtual node moved from " + hash + " to " + newHash + ": ";

            // Recheck the position of the old virtual node, since index may have changed after adding at the new hash position
            index = Collections.binarySearch(lookupTable.getTable(), node);
            // Get the successors and predecessors of the virtual node at the old hash
            List<VirtualNode> oldSuccessors = new ArrayList<>();
            List<VirtualNode> oldPredecessors = new ArrayList<>();
            for (int i = 0; i < ProxyServer.numOfReplicas; i++) {
                VirtualNode next = lookupTable.getTable().next(index + i);
                oldSuccessors.add(next);
                VirtualNode pre = lookupTable.getTable().pre(index - i);
                oldPredecessors.add(pre);
            }

            // get the successors and predecessors of the virtual node at the new hash
            List<VirtualNode> newSuccessors = new ArrayList<>();
            List<VirtualNode> newPredecessors = new ArrayList<>();
            // Get the index of the new virtual node in the BinarySearchList
            int newIndex = Collections.binarySearch(lookupTable.getTable(), newNode);
            for (int j = 0; j < ProxyServer.numOfReplicas; j++) {
                VirtualNode next = lookupTable.getTable().next(newIndex + j);
                newSuccessors.add(next);
                VirtualNode pre = lookupTable.getTable().pre(newIndex - j);
                newPredecessors.add(pre);
            }


            for (int i = 0; i < ProxyServer.numOfReplicas; i++) {
                VirtualNode oldSuccessor = oldSuccessors.get(i);
                if ((!newSuccessors.contains(oldSuccessor)) && (oldSuccessor.getHash() != newHash)) {
                    if (i != (ProxyServer.numOfReplicas - 1)) {
                        result += dataTransfer(node, oldSuccessor,
                                oldPredecessors.get(ProxyServer.numOfReplicas - 1 - i).getHash() + 1,
                                oldPredecessors.get(ProxyServer.numOfReplicas - 2 - i).getHash());
                    }
                    else{
                        result += dataTransfer(node, oldSuccessor,
                                oldPredecessors.get(ProxyServer.numOfReplicas - 1 - i).getHash() + 1,
                                hash);
                    }
                }
            }
            for (int i = 0; i < ProxyServer.numOfReplicas; i++) {
                VirtualNode newSuccessor = newSuccessors.get(i);
                if ((!oldSuccessors.contains(newSuccessor)) && (newSuccessor.getHash() != hash)) {
                    if (i != (ProxyServer.numOfReplicas - 1)) {
                        result += dataTransfer(newSuccessor, newNode,
                                newPredecessors.get(ProxyServer.numOfReplicas - 1 - i).getHash() + 1,
                                newPredecessors.get(ProxyServer.numOfReplicas - 2 - i).getHash());
                    }
                    else {
                        result += dataTransfer(newSuccessor, newNode,
                                newPredecessors.get(ProxyServer.numOfReplicas - 1 - i).getHash() + 1,
                                newHash);
                    }
                }
            }

            // Delete the virtual node at the old hash
            lookupTable.getTable().remove(index);
        }
        //Update the timestamp in lookupTable
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        lookupTable.setEpoch(timestamp.getTime());
        return result;
    }

//    public int hashFunction(String s){
//        int hash = String.valueOf(s).hashCode() % Hashing.MAX_HASH;
//        return hash;
//    }
//    public void writeRequest(String key){
//        int hash = hashFunction(key);
//        VirtualNode hashValue = new VirtualNode(hashFunction(key));
//        VirtualNode vNode = lookupTable.getTable().find(hashValue);
//        // Store replica in two successors
//        VirtualNode replica_1 = lookupTable.getTable().next(vNode);
//        VirtualNode replica_2 = lookupTable.getTable().next(replica_1);
//        write(vNode, hash);
//        write(replica_1, hash);
//        write(replica_2, hash);
//        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
//        lookupTable.setEpoch(timestamp.getTime());
//
//    }
//
//    // A helper function for writeRequest() method
//    public void write(VirtualNode virtualNode, int hash){
//        String address = lookupTable.getPhysicalNodeMap().get(virtualNode.getPhysicalNodeId()).getAddress() + " (port: " +
//                lookupTable.getPhysicalNodeMap().get(virtualNode.getPhysicalNodeId()).getPort() + ")";
//        System.out.println("\nConnecting to " + address + " to write on virtual node " + virtualNode.getHash() +
//                " for hash value " + hash);
//        System.out.println("Writing completed");
//    }
//
//    public void readRequest(String key){
//        int hash = hashFunction(key);
//        VirtualNode hashValue = new VirtualNode();
//        VirtualNode vNode = lookupTable.getTable().find(hashValue);
//        VirtualNode replica_1 = lookupTable.getTable().next(vNode);
//        VirtualNode replica_2 = lookupTable.getTable().next(replica_1);
//        read(vNode, hash);
//        read(replica_1, hash);
//        read(replica_2, hash);
//    }
//    public void read(VirtualNode virtualNode, int hash){
//        String address = lookupTable.getPhysicalNodeMap().get(virtualNode.getPhysicalNodeId()).getAddress() + " (port: " +
//                lookupTable.getPhysicalNodeMap().get(virtualNode.getPhysicalNodeId()).getPort() + ")";
//        System.out.println("\nConnecting to " + address + " to read for hash value " + hash);
//        System.out.println("Reading completed");
//
//    }

}
