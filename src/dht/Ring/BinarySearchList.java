package dht.Ring;

import java.util.*;

import dht.common.Hashing;

public class BinarySearchList extends ArrayList<VirtualNode> {
	boolean[] occupied = new boolean[ProxyServer.hashRange];
    
    public boolean checkExist(int hash) {
    	VirtualNode vNode = new VirtualNode(hash);
    	int index = Collections.binarySearch(this, vNode);
        if (index < 0) {
        	return false;
        }
        else {
        	return true;
        }
    }
    
    public int getRanHash() {
    	Random ran = new Random();
    	int hash = ran.nextInt(ProxyServer.hashRange);
    	int count = 0;
    	while(checkExist(hash)) {
    		hash = ran.nextInt(ProxyServer.hashRange);
    		count++;
    		
    		if (count > ProxyServer.hashRange / 10) {
    			break;
    		}
    	}
    	
    	if (count > 1000) {
    		boolean isfound = false;
    		for(int i = 0; i < occupied.length; i++) {
    			if (!occupied[i]) {
    				hash = i;
    				isfound = true;
    				break;
    			}
    		}
    		if (!isfound) {
    			hash = -1;
    		}
    	}
    	
    	return hash;
    }
    /**
     * @Param  Node to be added
     * @return false if virtual node with the same hash is already in list
     *          true if insertion succeed
     *
     *          This function first uses binary search {@see Collections.binarySearch} to locate where the new
     *          node should be added to.
     *
     *          Collections.binarySearch returns non-negative index if the node is found.
     *          Otherwise, returns -(insertion point) - 1.
     *
     *          Time Complexity O(log n)
     */
    @Override
    public boolean add(VirtualNode t) {
        int index = Collections.binarySearch(this, t);

        if (index >= 0) {
            // virtual node is already in the list
            return false;
        }
        else {
            index = -(index + 1);
            this.add(index, t);
            t.setIndex(index);
//            occupied[t.getHash()] = true;
            return true;
        }
    }

    /**
     * @Param  Node dummy node with hash
     * @return the node where the hash is hosted.
     *
     *          This function uses binary search {@see Collections.binarySearch} to locate the
     *          host node of the given hash.
     *
     *          Collections.binarySearch returns non-negative index if the node is found.
     *          Otherwise, returns -(insertion point) - 1.
     *
     *          If the index is negative, -(index + 1) is the index of host node.
     *          If the index is greater than the size of list, the host is the first node in list.
     *          Otherwise, the index is the actual index of the host
     *
     *
     *          Time Complexity O(log n)
     */
    public VirtualNode find(int hash) {
    	VirtualNode target = new VirtualNode(hash);
        int index = Collections.binarySearch(this, target);

        if (index < 0)
            index = -(index + 1);
        if (index >= size())
            index = 0;

        target = get(index);
        return target;
    }
    
    public VirtualNode find(VirtualNode node) {
        int index = Collections.binarySearch(this, node);

        if (index < 0)
            index = -(index + 1);
        if (index >= size())
            index = 0;

        node = get(index);
        return node;
    }

    /**
     * @Param  index
     * @return node of the given index
     *
     *          Index is cached to the node, for fast access of its successor.
     */
    @Override
    public VirtualNode get(int index) {
        if (index < 0)
            index = size() + index;
        else if (index >= size())
            index = index % size();
        VirtualNode node = super.get(index);
        //node.setIndex(index); // set current index in the table, for fast access to successor and predecessor

        return node;
    }

    /**
     * @Param  Node source node
     * @return the successor of the given node
     *
     *          Time Complexity O(1)
     */
    public VirtualNode next(VirtualNode node) {
        int index = Collections.binarySearch(this, node);
        return next(index);
    }

    public VirtualNode next(int index) {
        if (index + 1 == size()) // current node is the last element in list
            return get(0);
        else if (index + 1 > size())
            return get((index + 1) % size());
        else
            return get(index + 1);
    }

    /**
     * @Param  Node source node
     * @return the predecessor of the given node
     *
     *          Time Complexity O(1)
     */
    public VirtualNode pre(VirtualNode node) {
        int index = Collections.binarySearch(this, node);
        return pre(index);
    }
    public VirtualNode pre(int index) {
    	if (index < 0) {
                index--;
    		index = size() + index;
    		index = index % size();
    		return get(index);
    	}
    	else if (index == 0) // current node is the  first element in list
            return get(size() - 1);
        else
            return get(index - 1);
    }
    
    public VirtualNode getVirtualNode(String keyword) {
    	int rawHash = Hashing.getHashValFromKeyword(keyword, ProxyServer.hashRange);
    	VirtualNode node = find(rawHash);
    	return node;
    }
    
    public String getPhysicalNode(String keyword) {
    	VirtualNode node = getVirtualNode(keyword);
    	return node.getPhysicalNodeId();
    }
    
    public List<VirtualNode> getSuccessors(int rawHash) {
    	VirtualNode vNode = find(rawHash);
    	int index = vNode.getIndex();
        List<VirtualNode> successors = new ArrayList<>();
        for (int i = 0; i < ProxyServer.numOfReplicas; i++){
            VirtualNode next = next(index + i);
            successors.add(next);
        }
        
        return successors;
    }
    
    public List<VirtualNode> getSuccessors(String keyword) {
    	VirtualNode vNode = getVirtualNode(keyword);
    	int index = vNode.getIndex();
        List<VirtualNode> successors = new ArrayList<>();
        for (int i = 0; i < ProxyServer.numOfReplicas; i++){
            VirtualNode next = next(index + i);
            successors.add(next);
        }
        
        return successors;
    }
    
    public List<VirtualNode> getVirtualNodes(String keyword) {
    	VirtualNode vNode = getVirtualNode(keyword);
        List<VirtualNode> successors = getSuccessors(keyword);
        successors.add(0, vNode);
        
        return successors;
    }
    
    public List<VirtualNode> getVirtualNodes(int rawHash) {
    	VirtualNode vNode = find(rawHash);
        List<VirtualNode> successors = getSuccessors(rawHash);
        successors.add(0, vNode);
        
        return successors;
    }
    
    public int[] getVirtualNodeIds(String keyword) {
        List<VirtualNode> virtualNodes = getVirtualNodes(keyword);
        
        int[] virtualNodeIds = new int[virtualNodes.size() + 1];
        for (int i = 0; i < virtualNodes.size(); i++){
        	virtualNodeIds[i] = virtualNodes.get(i).getHash();
        }
        
        return virtualNodeIds;
    }
    
    public int[] getVirtualNodeIds(int rawHash) {
        List<VirtualNode> virtualNodes = getVirtualNodes(rawHash);
        
        int[] virtualNodeIds = new int[virtualNodes.size()];
        for (int i = 0; i < virtualNodes.size(); i++){
        	virtualNodeIds[i] = virtualNodes.get(i).getHash();
        }
        
        return virtualNodeIds;
    }

    public void updateIndex() {
    	if (this.size() > 0) {
    		for(int i = 0; i < this.size(); i++) {
    			this.get(i).setIndex(i);
    		}
    	}
    }
    
    public void updateIndex(int index) {
    	if (this.size() > 0) {
    		for(int i = index; i < this.size(); i++) {
    			this.get(i).setIndex(i);
    		}
    	}
    }

    public void updateIndex(int startIndex, int endIndex) {
        if (endIndex == this.size())
            endIndex--;
        if (this.size() > 0) {
            for(int i = startIndex; i <= endIndex; i++) {
                this.get(i).setIndex(i);
            }
        }
    }
}
