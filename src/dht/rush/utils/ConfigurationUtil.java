package dht.rush.utils;

import dht.rush.clusters.*;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.util.*;

public class ConfigurationUtil {
    public static ClusterStructureMap parseConfig(Document document) {
//        SAXReader reader = new SAXReader();
        Cluster root;

        Map<String, Cluster> clusterList = new HashMap<>();

        try {
//            File file = new File(path);
//            Document document = reader.read(file);
            Element rootElement = document.getRootElement();

            // Generate all nodes
            String rootIp = rootElement.element("proxy").element("ip").getStringValue();
            String rootPort = rootElement.element("proxy").element("port").getStringValue();

            int placementGroupNumber = Integer.parseInt(rootElement.element("placementGroupNumber").getStringValue());
            RushUtil.setNumberOfPlacementGroup(placementGroupNumber);

            int numberOfReplicas = Integer.parseInt(rootElement.element("replicationDegree").getStringValue());
            RushUtil.setNumberOfReplicas(numberOfReplicas);

            int numberOfCommands = Integer.parseInt(rootElement.element("commandNumber").getStringValue());
            RushUtil.setNumberOfCommands(numberOfCommands);

            double unitWeight = Double.parseDouble(rootElement.element("weight").getStringValue());
            int subClusterChildrenNumber = Integer.parseInt(rootElement.element("offset").getStringValue());

            Element subClusters = rootElement.element("subClusters");
            List subClusterList = subClusters.elements();

            int numberOfRootChildren = subClusterList.size();
            String rootId = "R";
            double rootWeight = 0;
            int j = 0; // physical node index

            for (int i = 0; i < numberOfRootChildren; i++) {
                Element subClusterElement = ((Element) subClusterList.get(i));
                List<Element> nodesList = ((Element) subClusterList.get(i)).elements();
//                int subClusterChildrenNumber = Integer.parseInt(subClusterElement.element("offset").getStringValue());
                String subClusterId = "S" + i;

//                double unitWeight = Double.parseDouble(subClusterElement.element("weight").getStringValue());

                double subClusterWeight = subClusterChildrenNumber * unitWeight;

                String subClusterIp = subClusterElement.element("ip").getStringValue();
                int subClusterPort = Integer.parseInt(subClusterElement.element("port").getStringValue());

                for (int index = 0; index < subClusterChildrenNumber; index++) {
                    String nodeId = "N" + j;
                    String nodePort = String.valueOf(subClusterPort + index);
                    Cluster physicalNode = new PhysicalNode(nodeId, subClusterIp, nodePort, subClusterId, 0, unitWeight, true, 100);
                    clusterList.put(nodeId, physicalNode);
                    j++;
                }
                rootWeight += subClusterWeight;
                Cluster subCluster = new SubCluster(subClusterId, subClusterIp, "", rootId, subClusterChildrenNumber, subClusterWeight, true);
                clusterList.put(subClusterId, subCluster);
            }

            root = new Root(rootId, rootIp, rootPort, "", numberOfRootChildren, rootWeight, true);
            clusterList.put(rootId, root);

            // Build tree
            Set<Map.Entry<String, Cluster>> entrySets = clusterList.entrySet();
            Cluster fakeParentOfRoot = new Cluster();

            for (Map.Entry<String, Cluster> en : entrySets) {
                Cluster c = en.getValue();
                String pid = c.getParentId();

                if (!pid.equals("")) {
                    Cluster parent = clusterList.get(pid);
                    parent.getSubClusters().add(c);
                    ClusterStructureMap parentCachedTreeStructure = parent.getCachedTreeStructure();
                    parentCachedTreeStructure.getChildrenList().put(c.getId(), c);
                } else {
                    fakeParentOfRoot.getCachedTreeStructure().getChildrenList().put(c.getId(), c);
                }
            }

            for (int idx = 0; idx < placementGroupNumber; idx++) {
                String id = "PG" + idx;
                fakeParentOfRoot.getCachedTreeStructure().getNodes(id);
            }

            fakeParentOfRoot.getCachedTreeStructure().setEpoch(0);
//            root.setCachedTreeStructure(fakeParentOfRoot.getCachedTreeStructure());
            return fakeParentOfRoot.getCachedTreeStructure();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
