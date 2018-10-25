package dht.elastic_DHT_centralized;

import javafx.util.Pair;

import java.util.LinkedList;
import java.util.List;

public class Init_Nodes {
    public void initialize(int totalHashSlots, List<String> activeIPs){
//        int numOfActiveNodes = activeIPs.size();
//        int slotsPerNode = totalHashSlots / numOfActiveNodes;
//        List<PhysicalNode> activeNodes = new LinkedList<>();
//        String[] routeTable= new String[totalHashSlots];
//
//        for (int i = 0; i < numOfActiveNodes; i++){
//            //For each data node, their unique ID is of the form D###
//            //where ### is the last three-digit number of the ip address.
//            String id = "D" + activeIPs.get(i).substring(activeIPs.get(i).length() - 3);
//
//            // Initially, hash slots are evenly distributed among active nodes
//            int start = i * slotsPerNode + 0;
//            int end = i * slotsPerNode + slotsPerNode;
//            Pair p = new Pair(start, end);
//            LinkedList<Pair<Integer, Integer>> initialLoad = new LinkedList<>();
//            initialLoad.add(p);
//            PhysicalNode node = new PhysicalNode(id, activeIPs.get(i), initialLoad);
//            activeNodes.add(node);
//            for (int j = start; j < end; j++){
//                routeTable[j] = id;
//            }
//        }
//        Proxy proxy = new Proxy();
//        proxy.setRouteTable(routeTable);
//        proxy.setActiveNodes(activeNodes);
    }

}
