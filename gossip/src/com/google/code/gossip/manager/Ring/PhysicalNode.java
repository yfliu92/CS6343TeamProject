package com.google.code.gossip.manager.Ring;

import com.google.code.gossip.manager.Ring.Hashing;
import com.google.code.gossip.manager.Ring.BinarySearchList;
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

public class PhysicalNode implements Comparable<PhysicalNode>{

    protected String id;

    protected String address;

    protected int port;
    
    public int start_vir;
    public int end_vir;
    public int back_start_vir;
    public int back_end_vir;

    private String status;

    public BinarySearchList physicalNodes;
    public long epoch;

    public final static String STATUS_ACTIVE = "active";

    public final static String STATUS_INACTIVE = "inactive";

    public PhysicalNode() {
    }

    public PhysicalNode(int hash)
    {
        this.start_vir = hash;
        this.end_vir = hash;
    }

    public PhysicalNode(String ip, int port){
        this.address = ip;
        this.port = port;
        this.id = ip + "-" + Integer.toString(port);
        this.status = STATUS_ACTIVE;
        int hash = -1;
        if(physicalNodes != null)
        {
            hash = physicalNodes.getRanHash();
        }
        this.end_vir = hash;
        this.start_vir = hash;
    }

    public PhysicalNode(String ip, int port, int hash){
        this.address = ip;
        this.port = port;
        this.id = ip + "-" + Integer.toString(port);
        this.status = STATUS_ACTIVE;
        this.end_vir = hash;
        this.start_vir = hash;
    }

    public PhysicalNode(String ip, int port, int start_vir, int end_vir){
        this.address = ip;
        this.port = port;
        this.id = ip + "-" + Integer.toString(port);
        this.status = STATUS_ACTIVE;
        this.end_vir = end_vir;
        this.start_vir = start_vir;
    }

    public PhysicalNode(String ip, int port, int start_vir, int end_vir, int back_start_vir, int back_end_vir){
        this.address = ip;
        this.port = port;
        this.id = ip + "-" + Integer.toString(port);
        this.status = STATUS_ACTIVE;
        this.end_vir = end_vir;
        this.start_vir = start_vir;
        this.back_start_vir = back_start_vir;
        this.back_end_vir = back_end_vir;
    }

    public void setInfo(String id, int start_vir, int end_vir, int back_start_vir, int back_end_vir)
    {
        this.id = id;
        this.status = STATUS_ACTIVE;
        this.end_vir = end_vir;
        this.start_vir = start_vir;
        this.back_start_vir = back_start_vir;
        this.back_end_vir = back_end_vir;
        this.address = id.split("-")[0]; 
        this.port = Integer.parseInt(id.split("-")[1]);
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

    public String listNodes() {
    	return physicalNodes.serialize();
    }

    public int getHash()
    {
        return this.start_vir;
    }

    public long getEpoch()
    {
        return this.epoch;
    }

    public String read(int hash)
    {
        if(hash >= this.start_vir && hash <= this.end_vir)
            return "read success";
        if(hash >= this.back_start_vir && hash <= this.back_end_vir)
            return "read success";
        else
            return "read failure, cannot find in Physical node:" + this.id;
    }

    public String write(int hash)
    {
        List<PhysicalNode> rep_nodes = physicalNodes.getPhysicalNodes(hash);
        String tmp = "";
        for(PhysicalNode item:rep_nodes)
            tmp = tmp + item.getId() + " ";
        if(hash >= this.start_vir && hash <= this.end_vir)
        {
            return "write success. " + tmp;
        }
        else if(hash >= this.start_vir && hash <= this.end_vir)
        {
            return "write success. " + tmp;
        }
        else
            return "write failure, cannot find in Physical node for Vritual node:" + hash;
    }

    public String print()
    {   
        if (physicalNodes.size() == 0) {
            System.out.println("No data found in the table");
        }   
        else {
            System.out.println("epoch number: " + this.epoch);
            for(PhysicalNode node: physicalNodes) {
                System.out.println(node.toJSON().toString());
            }   
        }
        return String.valueOf(this.epoch);
    }
    
    public String addNode(String ip, int port) {
    	String result = "";
        int hash = physicalNodes.getRanHash();
        //System.out.println("getRanHash:" + hash);
        
    	if (hash >= 0) {
    		try {
        		String tmp = addNode(ip, port, hash);
        		result = "true|Node added successfully, hash " + hash + tmp;
    		}
    		catch(Exception ee) {
    			result = "false|exception when adding node (with hash " + hash + ") - " + ee.toString();
                System.out.println(result);
    		}
    	}
    	else {
    		result = "false|Physical node exhausted";
    	}
    	return result;
    }

    public String dataTransfer(int fromNode, int toNode, int start, int end)
    {
        StringBuilder result = new StringBuilder();
        String address1 = " (" + physicalNodes.get(fromNode).getId() + ")";
        String address2 = " (" + physicalNodes.get(toNode).getId() + ")";
        result.append("\nTransfering main data from physical node:" + address1 +  " to physical node:" + address2 + ": " + "Transferring virtual nodes (" + start + ", " + end + ")");
        if(start == physicalNodes.get(fromNode).start_vir)
            physicalNodes.get(fromNode).start_vir = physicalNodes.next(end);
        else if(end == physicalNodes.get(fromNode).end_vir)
            physicalNodes.get(fromNode).end_vir = physicalNodes.pre(start);
        else
            result.append("Failed. fromNode:" + physicalNodes.get(fromNode).start_vir + "~" +  physicalNodes.get(fromNode).end_vir + " start:" + start + " end:" + end);
        if(physicalNodes.next(end) == physicalNodes.get(toNode).start_vir || end == physicalNodes.get(toNode).start_vir)
            physicalNodes.get(toNode).start_vir = start;
        else if(physicalNodes.pre(start) == physicalNodes.get(toNode).end_vir || start == physicalNodes.get(toNode).end_vir)
            physicalNodes.get(toNode).end_vir = end;
        else
            result.append("Failed. fromNode:" + physicalNodes.get(fromNode).start_vir + "~" +  physicalNodes.get(fromNode).end_vir + " start:" + start + " end:" + end);
        return result.toString();
    }

    public String backupTransfer(int fromNode, int toNode, int start, int end)
    {
        StringBuilder result = new StringBuilder();
        String address1 = " (" + physicalNodes.get(fromNode).getId() + ")";
        String address2 = " (" + physicalNodes.get(toNode).getId() + ")";
        result.append("\nTransfering backup data from physical node:" + address1 +  " to physical node:" + address2 + ": " + "Transferring virtual nodes (" + start + ", " + end + ")");
        if(start == physicalNodes.get(fromNode).back_start_vir)
            physicalNodes.get(fromNode).back_start_vir = physicalNodes.next(end);
        else if(end == physicalNodes.get(fromNode).back_end_vir)
            physicalNodes.get(fromNode).back_end_vir = physicalNodes.pre(start);
        else
            result.append("Failed. fromNode:" + physicalNodes.get(fromNode).start_vir + "~" +  physicalNodes.get(fromNode).end_vir + " start:" + start + " end:" + end);

        if(physicalNodes.get(toNode).back_start_vir == 0 && physicalNodes.get(toNode).back_start_vir == 0)
        {
            physicalNodes.get(toNode).back_start_vir = start;
            physicalNodes.get(toNode).back_end_vir = end;
        }
        else if(physicalNodes.next(end) == physicalNodes.get(toNode).back_start_vir)
            physicalNodes.get(toNode).back_start_vir = start;
        else if(physicalNodes.pre(start) == physicalNodes.get(toNode).back_end_vir)
            physicalNodes.get(toNode).back_end_vir = end;
        else
            result.append("Failed. fromNode:" + physicalNodes.get(fromNode).start_vir + "~" +  physicalNodes.get(fromNode).end_vir + " start:" + start + " end:" + end);
        return result.toString();
    }

    public String balanceTransfer(int fromNode, int toNode, int start, int end)
    {
        StringBuilder result = new StringBuilder();
        String address1 = " (" + physicalNodes.get(fromNode).getId() + ")";
        String address2 = " (" + physicalNodes.get(toNode).getId() + ")";
        result.append("\nTransfering balance data from physical node:" + address1 +  " to physical node:" + address2 + ": " + "Transferring virtual nodes (" + start + ", " + end + ")");
        if(start == physicalNodes.get(fromNode).start_vir)
            physicalNodes.get(fromNode).start_vir = physicalNodes.next(end);
        else if(end == physicalNodes.get(fromNode).end_vir)
            physicalNodes.get(fromNode).end_vir = physicalNodes.pre(start);
        else
            result.append("Failed. fromNode:" + physicalNodes.get(fromNode).start_vir + "~" +  physicalNodes.get(fromNode).end_vir + " start:" + start + " end:" + end);

        if(physicalNodes.next(end) == physicalNodes.get(toNode).start_vir)
            physicalNodes.get(toNode).start_vir = start;
        else if(physicalNodes.pre(start) == physicalNodes.get(toNode).end_vir)
            physicalNodes.get(toNode).end_vir = end;
        else
            result.append("Failed. fromNode:" + physicalNodes.get(fromNode).start_vir + "~" +  physicalNodes.get(fromNode).end_vir + " start:" + start + " end:" + end);
        
        result.append(backupTransfer(toNode, physicalNodes.next(toNode, ProxyServer.numOfReplicas - 1), start, end));
        return result.toString();
    }

    //Add a physical node that maps to just 1 virtual node
    public String addNode(String ip, int port, int hash){
        if(ProxyServer.phy_nodes_num >= ProxyServer.vir_nodes_num / 2)
        {
            System.out.println("Too many Physical Nodes:" + ProxyServer.phy_nodes_num);
            return "cannot add Physical nodes, there are too many Physical nodes";
        }
        PhysicalNode node = new PhysicalNode(ip,port,hash);
        String result = physicalNodes.addNode(node);
        ProxyServer.phy_nodes_num = ProxyServer.phy_nodes_num + 1;
        return result;
    }

    public String updateNode(String info)
    {
        String result = physicalNodes.updateNode(info);
        return result;
    }

    // Delete virtual node by its hash value
    public String deleteNode(int index)
    {
        PhysicalNode node = physicalNodes.get(index);
        return this.deleteNode(node);
    }

    public String deleteNode(PhysicalNode node)
    {
        String result =  physicalNodes.deleteNode(node);
        ProxyServer.phy_nodes_num = ProxyServer.phy_nodes_num - 1;
        return result;
    }

    public String failNode(String ip, int port){
        String failed_id = ip + "-" + Integer.toString(port);
        PhysicalNode node = physicalNodes.getPhysicalNode(failed_id);
        node.setStatus("inactive");
        String result = deleteNode(node);
        // Set the virtual node list of the failed node to be empty
        return result;
    }

    // Change the position of a virtual node on the ring to balance load
    public String loadBalance(int hash) { // move the node clockwise if delta > 0, counterclockwise if delta < 0
        return physicalNodes.loadBalance(hash);
    }

    public JsonObject toJSON() {
        JsonObject jsonObj = Json.createObjectBuilder()
                .add("id",this.id)
                .add("start_vir",this.start_vir)
                .add("end_vir",this.end_vir)
                .add("backup_start_vir", this.back_start_vir)
                .add("backup_end_vir", this.back_end_vir)
                .build();
        return jsonObj;
    }

    public int compareTo(PhysicalNode o) {
        if(this.start_vir <= o.start_vir && this.end_vir >= o.end_vir)
        {
            return 0;
        }
        else if(this.end_vir < o.start_vir)
        {
            return -1;
        }
        else if(this.start_vir > o.end_vir)
        {
            return 1;
        }
        else
        {
            System.out.println("PhysicalNode value Bug:");
            System.out.println(this.toJSON().toString());
            System.out.println(o.toJSON().toString());
            System.exit(0);
        }
        return 0;
    }
}
