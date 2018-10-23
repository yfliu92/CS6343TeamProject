package dht.Ring;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.*;

import dht.common.Hashing;

public class BinarySearchList extends ArrayList<Indexable> {
	boolean[] occupied = new boolean[Hashing.MAX_HASH];
	List<Indexable> list;

	public BinarySearchList() {
		list = new ArrayList<Indexable>();
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
    public boolean add(Indexable t) {
        int index = Collections.binarySearch(this.list, t);

        if (index >= 0) {
            // virtual node is already in the list
            return false;
        }
        else {
            index = -(index + 1);
            this.list.add(index, t);
            t.setIndex(index);
            occupied[t.getHash()] = true;
            return true;
        }
    }
    
    public boolean checkExist(int hash) {
    	VirtualNode vNode = new VirtualNode(hash);
    	int index = Collections.binarySearch(this.list, vNode);
        if (index < 0) {
        	return false;
        }
        else {
        	return true;
        }
    }
    
    public int getRanHash() {
    	Random ran = new Random();
    	int hash = ran.nextInt(Hashing.MAX_HASH);
    	int count = 0;
    	while(checkExist(hash)) {
    		hash = ran.nextInt(Hashing.MAX_HASH);
    		count++;
    		
    		if (count > Hashing.MAX_HASH / 10) {
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
    public Indexable find(Indexable node) {
        int index = Collections.binarySearch(this.list, node);

        if (index < 0)
            index = -(index + 1);
        if (index >= this.list.size())
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
    public Indexable get(int index) {
//<<<<<<< HEAD
//        if (index < 0) {
////        	index = this.list.size() - index;
//        	index = 0;
//        }
//            
////        Indexable node = super.get(index);
//        Indexable node = this.list.get(index);
//=======
        if (index < 0)
            index = this.list.size() + index;
        else if (index >= this.list.size())
            index = index % this.list.size();
        Indexable node = this.list.get(index);
//>>>>>>> fdc2aa80cf46d1adaa66f2886193a35b15a776c7
        //node.setIndex(index); // set current index in the table, for fast access to successor and predecessor

        return node;
    }

    /**
     * @Param  Node source node
     * @return the successor of the given node
     *
     *          Time Complexity O(1)
     */
    public Indexable next(Indexable node) {
        int index = Collections.binarySearch(this.list, node);
        return next(index);
    }

    public Indexable next(int index) {
//<<<<<<< HEAD
//        if (index + 1 >= this.list.size()) // current node is the last element in list 
//        {
//        	System.out.println("big index " + index + " size " + this.list.size());
//        	return get(0);
//        }
//        else {
//        	System.out.println("index " + index + " size " + this.list.size());
//=======
        if (index + 1 == this.list.size()) // current node is the last element in list
            return get(0);
        else if (index + 1 > this.list.size())
            return get((index + 1) % this.list.size());
        else
//>>>>>>> fdc2aa80cf46d1adaa66f2886193a35b15a776c7
            return get(index + 1);
//        }
    }

    /**
     * @Param  Node source node
     * @return the predecessor of the given node
     *
     *          Time Complexity O(1)
     */
    public Indexable pre(Indexable node) {
        int index = Collections.binarySearch(this.list, node);
        return pre(index);
    }
    public Indexable pre(int index) {
        if (index == 0) // current node is the  first element in list
            return get(this.list.size() - 1);
        else
            return get(index - 1);
    }
}
