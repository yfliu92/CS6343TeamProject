package com.google.code.gossip.manager.Ring;

import java.util.*;
import javax.json.*;
import java.io.*;

import com.google.code.gossip.manager.Ring.Hashing;

public class BinarySearchList extends ArrayList<PhysicalNode> {
    private ProxyServer proxy;
    public int epoch;

    public BinarySearchList(int epoch)
    {
        this.epoch = epoch;
    }

    public BinarySearchList(ProxyServer proxy)
    {
        this.proxy = proxy;
        epoch = 0;
    }

    public boolean checkExist(int hash) {
    	PhysicalNode vNode = new PhysicalNode(hash);
    	int index = Collections.binarySearch(this, vNode);
        //System.out.println("index:" + index);
        if (get(index).start_vir != hash) {
        	return false;
        }
        else {
        	return true;
        }
    }

    public int getRanHash() {
        Random ran = new Random();
        int hash = ran.nextInt(Hashing.MAX_HASH);
        while(checkExist(hash)) {
            hash = ran.nextInt(Hashing.MAX_HASH);
        }
        return hash;
    }   

    public int find(int hash) {
    	PhysicalNode target = new PhysicalNode(hash);
        //System.out.println("PhysicalNode.find, hash:" + hash);
        int index = Collections.binarySearch(this, target);
        //System.out.println("PhysicalNode.find, hash:" + index);
        if (index < 0 || index >= size())
        {
            System.out.println("PhysicalNode.find error, hash:" + hash);
            //System.exit(0);
        }
        return index;
    }
    
    public int find(PhysicalNode node) {
        int index = Collections.binarySearch(this, node);
        if (index < 0 || index >= size())
        {
            System.out.println("PhysicalNode.find error, hash:" + node.start_vir);
            //System.exit(0);
        }
        return index;
    }

    @Override
    public PhysicalNode get(int index) {
        if (index < 0 || index >= size())
        {
            System.out.println("PhysicalNode.get error, index:" + index);
            System.exit(0);
        }
        PhysicalNode node = super.get(index);

        return node;
    }

    public int next(PhysicalNode node) {
        int index = Collections.binarySearch(this, node);
        return next(index);
    }

    public int next(int index) {
        if (index + 1 == size()) // current node is the last element in list
            return 0;
        else
            return index + 1;
    }

    public int next(int index, int count) {
        if (index + count >= size()) // current node is the last element in list
            return index + count - size();
        else
            return index + count;
    }

    public int pre(int index) {
    	if (index == 0) // current node is the  first element in list
            return size() - 1;
        else
            return index - 1;
    }
    
    public int next_vir(int index, int count) {
        if (index + count >= ProxyServer.vir_nodes_num) // current node is the last element in list
            return index + count - ProxyServer.vir_nodes_num;
        else
            return index + count;
    }

    public int pre(int index, int count) {
    	if (index < count) // current node is the  first element in list
            return size() - count + index;
        else
            return index - count;
    }

    public int pre_vir(int index, int count) {
    	if (index < count) // current node is the  first element in list
            return ProxyServer.vir_nodes_num - count + index;
        else
            return index - count;
    }

    public PhysicalNode getPhysicalNode(String keyword) {
        for(PhysicalNode node : this)
            if(node.getId() == keyword)
    	        return node;
        return null;
    }
    
    public List<PhysicalNode> getSuccessors(int index) {
        List<PhysicalNode> successors = new ArrayList<>();
        for (int i = 0; i < ProxyServer.numOfReplicas; i++){
            int next = next(index + i);
            successors.add(get(next));
        }
        return successors;
    }
    
    public List<PhysicalNode> getSuccessors(String keyword) {
    	PhysicalNode node = getPhysicalNode(keyword);
    	int index = find(node);
        List<PhysicalNode> successors = new ArrayList<>();
        for (int i = 0; i < ProxyServer.numOfReplicas; i++){
            int next = next(index + i);
            successors.add(this.get(next));
        }
        return successors;
    }
    
    public List<PhysicalNode> getPhysicalNodes(String keyword) {
    	int[] tmp = getPhysicalNodeIds(keyword);
        List<PhysicalNode> successors = new ArrayList<>();
        for(int item:tmp)
            successors.add(get(item));
        return successors;
    }
    
    public List<PhysicalNode> getPhysicalNodes(int rawHash) {
        //System.out.println("rawHash:" + rawHash);
    	int[] tmp = getPhysicalNodeIds(rawHash);
        List<PhysicalNode> successors = new ArrayList<>();
        for(int item:tmp)
            successors.add(get(item));
        return successors;
    }
    
    public int[] getPhysicalNodeIds(String keyword) {
        int index = find(getPhysicalNode(keyword));
        int[] rlt = new int[ProxyServer.numOfReplicas];
        for(int i = 0; i < ProxyServer.numOfReplicas; i++)
        {
            rlt[i] = index;
            index = next(index);
        }
        return rlt;
    }
    
    public int[] getPhysicalNodeIds(int rawHash) {
        int index = find(rawHash);
        //System.out.println("index:" + index);
        int[] rlt = new int[ProxyServer.numOfReplicas];
        for(int i = 0; i < ProxyServer.numOfReplicas; i++)
        {
            rlt[i] = index;
            index = next(index);
        }
        return rlt;
    }
    
    public void updateIndex() {
        Collections.sort(this);
    }

    public String serialize() {
        return this.toJSON().toString();
    }   

    public JsonObject toJSON() {
        JsonObjectBuilder jsonBuilder = Json.createObjectBuilder();
     
        JsonArrayBuilder tableBuilder = Json.createArrayBuilder();
        for(PhysicalNode node: this) {
            tableBuilder.add(node.toJSON());
        }
        jsonBuilder.add("table", tableBuilder.build());
        jsonBuilder.add("virtualnodes",String.valueOf(proxy.start_vir) + "~" + String.valueOf(proxy.end_vir));
     
        return jsonBuilder.build();
    }

    public String addNode(PhysicalNode node)
    {
        this.epoch +=1;
        StringBuilder result = new StringBuilder();
        int prev_node = this.find(node);
        //System.out.println("prev_node:" + prev_node);
        int cur_node = next(prev_node);
        this.add(cur_node, node);

        result.append(proxy.dataTransfer(prev_node, cur_node, this.get(cur_node).start_vir, this.get(prev_node).end_vir));
        int next_node = next(cur_node);
        for(int i = 0; i < ProxyServer.numOfReplicas - 1; i++)
        {
            int first_backup = pre(next_node, ProxyServer.numOfReplicas);
            result.append(proxy.backupTransfer(next_node, cur_node, this.get(first_backup).start_vir, this.get(first_backup).end_vir));
            next_node = next(next_node);
        }
        return result.toString();
    }

    public String updateNode(String info)
    {
        String str;
        JsonObject jsonObject = null;
        //System.out.println(info);
        JsonReader jsonReader = Json.createReader(new StringReader(info));
        jsonObject = jsonReader.readObject();
        int epoch = jsonObject.getInt("epoch");
        if(this.epoch >= epoch)
        {
            return "Newest DHT Table, No need to update.";
        }
        String table = jsonObject.getJsonObject("jsonResult").get("table").toString();
        System.out.println(table);
        jsonReader = Json.createReader(new StringReader(table));
        JsonArray jsonArray = jsonReader.readArray();
        int n = jsonArray.size();
        int m = this.size();
        if(n == m)
        {}
        else if(n > m)
        {
            for(int i = 0; i < n - m; i++)
                this.add(new PhysicalNode());
        }
        else if(n < m)
        {
            for(int i = 0; i < m - n; i++)
                this.remove(0);
        }
        else
        {
            System.out.println("Unexpected situation: different BinarySearchList list size" + n + m);
        }
        for (int i = 0; i < n; i++) {
            JsonObject memberJSONObject = jsonArray.getJsonObject(i);
            // Now the array should contain 3 objects (hostname, port and heartbeat).
            if (memberJSONObject.size() == 5) {
                String id = memberJSONObject.getString("id");
                int start_vir = memberJSONObject.getInt("start_vir");
                int end_vir = memberJSONObject.getInt("end_vir");
                int back_start_vir = memberJSONObject.getInt("backup_start_vir");
                int back_end_vir = memberJSONObject.getInt("backup_end_vir");
                this.get(i).setInfo(id, start_vir, end_vir, back_start_vir, back_end_vir);
            } else {
                System.out.println("The received member object does not contain 3 objects:\n" + memberJSONObject.toString());
            }
        }
        return "Update DHT Table successed";
    }
    
    public String deleteNode(PhysicalNode node)
    {
        this.epoch +=1;
        StringBuilder result = new StringBuilder();
        int cur_node = this.find(node);
        int prev_node = pre(cur_node);
        result.append(proxy.dataTransfer(cur_node, prev_node, this.get(cur_node).start_vir, this.get(cur_node).end_vir));
        int next_node = next(cur_node);
        for(int i = 0; i < ProxyServer.numOfReplicas - 1 - 1; i++)
        {
            int first_backup = pre(next_node, ProxyServer.numOfReplicas);
            result.append(proxy.backupTransfer(cur_node, next_node, this.get(first_backup).start_vir, this.get(first_backup).end_vir));
            next_node = next(next_node);
        }
        int first_backup = pre(next_node, ProxyServer.numOfReplicas);
        result.append(proxy.backupTransfer(cur_node, next_node, this.get(first_backup).start_vir, this.get(cur_node).back_end_vir));
        this.remove(cur_node);
        return result.toString();
    }

    public String loadBalance(int hash)
    {
        this.epoch +=1;
        StringBuilder result = new StringBuilder();
        int cur_node = hash;
        int prev_node = pre(cur_node);
        int next_node = next(cur_node);
        int size = this.get(cur_node).end_vir - this.get(cur_node).start_vir + 1;
        if(size < 0) size = size + ProxyServer.vir_nodes_num + 1;
        int size_1 = this.get(prev_node).end_vir - this.get(prev_node).start_vir + 1;
        if(size_1 < 0) size_1 = size_1 + ProxyServer.vir_nodes_num + 1;
        int size_2 = this.get(next_node).end_vir - this.get(next_node).start_vir + 1;
        if(size_2 < 0) size_2 = size_2 + ProxyServer.vir_nodes_num + 1;
        System.out.println("size:" + size + "  size1:" + size_1 + "  size2:" + size_2);
        if (size_1 - size > ProxyServer.balance_level)
        {
            int count = (int)((size_1 - size) / 2);
            result.append(proxy.balanceTransfer(prev_node, cur_node, pre(get(prev_node).end_vir, count - 1), get(prev_node).end_vir));
            //loadBalance(prev_node);
        }
        if (size_2 - size > ProxyServer.balance_level)
        {
            int count = (int)((size_2 - size) / 2);
            result.append(proxy.balanceTransfer(next_node, cur_node, get(next_node).start_vir, next(get(prev_node).start_vir, count -1)));
            //loadBalance(next_node);
        }
        if (size - size_1 > ProxyServer.balance_level)
        {
            int count = (int)((size - size_1) / 2);
            result.append(proxy.balanceTransfer(cur_node, prev_node, get(cur_node).start_vir, next(get(prev_node).start_vir, count - 1)));
            //loadBalance(prev_node);
        }
        if (size - size_2 > ProxyServer.balance_level)
        {
            int count = (int)((size - size_2) / 2);
            result.append(proxy.balanceTransfer(cur_node, next_node, pre(get(cur_node).end_vir, count - 1), get(cur_node).end_vir));
            //loadBalance(next_node);
        }
        return result.toString();
    }
}
