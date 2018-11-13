package dht.rush.utils;

import dht.rush.clusters.Cluster;
import dht.rush.clusters.ClusterStructureMap;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class GenerateControlClientCommandUtil {
    private static ClusterStructureMap map;

    //    tip += "\ndeletenode <subClusterId> <IP> <Port>  //example: deletenode S0 localhost 689";
//    tip += "\naddnode <subClusterId> <IP> <Port> <weight>  //example: addnode S0 localhost 689 0.5";
    private static String[] commands = new String[]{"addnode", "deletenode", "changeweight"};
    private static String newNodeIp = "192.168.0.9";
    private static int portBase = 81000;
    private static int range = 1000;
    private static Set<Integer> portPool = new HashSet<>();

    public static void run() {
        String rootPath = System.getProperty("user.dir");
//        String path = rootPath + File.separator + "src" + File.separator + "dht" + File.separator + "rush" + File.separator + "cephControlClient.txt";
        String path = rootPath + File.separator + "dht" + File.separator + "rush" + File.separator + "cephControlClient.txt";

        try {

            File filename = new File(path);

            // create txt file first
            if (!filename.exists()) {
                filename.createNewFile();
            }

            int addNodePortOffset = 0;

            BufferedWriter writer = new BufferedWriter(new FileWriter(filename, true));
            int numberOfCommands = RushUtil.getNumberOfCommands();
            for (int i = 0; i < numberOfCommands; i++) {
                StringBuilder sb = new StringBuilder();

                // select command
                int commandIndex = new Random().nextInt(3);
                String command = commands[commandIndex];

                sb.append(command);

                // get sub cluster id
                String subClusterId = getSubClusterId();
                sb.append(" " + subClusterId);

                if (command.equals("addnode")) {
                    sb.append(" " + newNodeIp);

                    sb.append(" " + (portBase + addNodePortOffset) + " " + 1.0);

                    map.addPhysicalNode(subClusterId, newNodeIp, String.valueOf((portBase + addNodePortOffset)), 1.0);
                    addNodePortOffset += 1;

                } else if (command.equals("deletenode")) {

                    String ipAndPort = getActivePhysicalNodeIpAndPort(subClusterId);
                    sb.append(ipAndPort);
                    String[] data = ipAndPort.split(" ");
                    map.deletePhysicalNode(subClusterId, data[1], data[2]);

                } else if (command.equals("changeweight")) {
                    String ipAndPort = getActivePhysicalNodeIpAndPort(subClusterId);
                    sb.append(ipAndPort);
                    String[] data = ipAndPort.split(" ");
                    double weight = new Random().nextInt(10) * 1.0 + 1;
                    sb.append(" " + weight);

                    map.changeNodeWeight(subClusterId, data[1], data[2], weight);
                }
                String content = sb.toString();
                System.out.println("command " + i + ": " + content);
                writer.write(content + System.lineSeparator());
            }
            writer.close();
            System.out.println("Generating commands finished");

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static ClusterStructureMap getMap() {
        return map;
    }

    public static void setMap(ClusterStructureMap map) {
        GenerateControlClientCommandUtil.map = map;
    }

    public static String getSubClusterId() {
        Cluster root = map.getChildrenList().get("R");
        int childrenSize = root.getSubClusters().size();
        int index = new Random().nextInt(childrenSize);
        return root.getSubClusters().get(index).getId();
    }

    public static String getActivePhysicalNodeIpAndPort(String subId) {
        Cluster root = map.getChildrenList().get("R");
        Cluster sub = root.getCachedTreeStructure().getChildrenList().get(subId);
        int childrenSize = sub.getSubClusters().size();
        String ret = "";
        while (true) {
            int index = new Random().nextInt(childrenSize);
            Cluster node = sub.getSubClusters().get(index);
            if (node.getActive()) {
                ret += " " + node.getIp() + " " + node.getPort();
                break;
            }
        }
        return ret;
    }
}
