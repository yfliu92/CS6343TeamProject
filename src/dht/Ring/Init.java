package dht.Ring;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Init {
    public static void main(String[] args) {
        VirtualNode V0 = new VirtualNode(0, "D100-8001");
        VirtualNode V21 = new VirtualNode(21, "D100-8001");
        VirtualNode V42 = new VirtualNode(42, "D100-8001");
        List<VirtualNode> virtualNodes1 = new ArrayList<>();
        virtualNodes1.add(V0);
        virtualNodes1.add(V21);
        virtualNodes1.add(V42);
        PhysicalNode P1 = new PhysicalNode("D100-8001", "192.168.0.100", 8001, "active", virtualNodes1);

        VirtualNode V7 = new VirtualNode(7, "D100-8002");
        VirtualNode V28 = new VirtualNode(28, "D100-8002");
        VirtualNode V49 = new VirtualNode(49, "D100-8002");
        List<VirtualNode> virtualNodes2 = new ArrayList<>();
        virtualNodes2.add(V7);
        virtualNodes2.add(V28);
        virtualNodes2.add(V49);
        PhysicalNode P2 = new PhysicalNode("D100-8002", "192.168.0.100", 8002, "active", virtualNodes2);

        VirtualNode V14 = new VirtualNode(14, "D100-8003");
        VirtualNode V35 = new VirtualNode(35, "D100-8003");
        VirtualNode V56 = new VirtualNode(56, "D100-8003");
        List<VirtualNode> virtualNodes3 = new ArrayList<>();
        virtualNodes3.add(V14);
        virtualNodes3.add(V35);
        virtualNodes3.add(V56);
        PhysicalNode P3 = new PhysicalNode("D100-8003", "192.168.0.100", 8003, "active", virtualNodes3);

        LookupTable t = new LookupTable();
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        t.setEpoch(timestamp.getTime());
        BinarySearchList table = new BinarySearchList();
        table.add(V0);
        table.add(V7);
        table.add(V14);
        table.add(V21);
        table.add(V28);
        table.add(V35);
        table.add(V42);
        table.add(V49);
        table.add(V56);
        t.setTable(table);
        HashMap<String, PhysicalNode> physicalNodeMap = new HashMap<>();
        physicalNodeMap.put("D100-8001", P1);
        physicalNodeMap.put("D100-8002", P2);
        physicalNodeMap.put("D100-8003", P3);
        t.setPhysicalNodeMap(physicalNodeMap);
        P1.setLookupTable(t);
        P2.setLookupTable(t);
        P3.setLookupTable(t);




        P1.addNode("192.168.0.100", 8004, 44);

        P1.deleteNode(49);
//        HashMap<String, PhysicalNode> temp = P1.getLookupTable().getPhysicalNodeMap();
//        for (HashMap.Entry<String, PhysicalNode> entry : temp.entrySet()){
//            System.out.println(entry.getKey() + " "+ entry.getValue());
//        }
        //System.out.println(V28.getPhysicalNodeId());
        //System.out.println(P1.getLookupTable().getPhysicalNodeMap());

    }
}