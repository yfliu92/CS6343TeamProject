package dht.rush.utils;

import dht.rush.clusters.*;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.util.*;

public class ConfigurationUtil {
    public static ClusterStructureMap parseConfig(String path) {
        SAXReader reader = new SAXReader();
        Cluster root;

        Map<String, Cluster> clusterList = new HashMap<>();

        try {
            File file = new File(path);
            Document document = reader.read(file);
            Element rootElement = document.getRootElement();

            // Generate all nodes
            String rootIp = rootElement.element("ip").getStringValue();
            String rootPort = rootElement.element("port").getStringValue();

            int placementGroupNumber = Integer.parseInt(rootElement.element("placementGroupNumber").getStringValue());
            RushUtil.setNumberOfPlacementGroup(placementGroupNumber);

            int numberOfReplicas = Integer.parseInt(rootElement.element("replicationDegree").getStringValue());
            RushUtil.setNumberOfReplicas(numberOfReplicas);

            Element subClusters = rootElement.element("subClusters");
            List subClusterList = subClusters.elements();

            int numberOfRootChildren = subClusterList.size();
            String rootId = "R";
            double rootWeight = 0;
            int j = 0; // physical node index

            for (int i = 0; i < numberOfRootChildren; i++) {
                List<Element> nodesList = ((Element) subClusterList.get(i)).elements();
                int subClusterChildrenNumber = nodesList.size();
                String subClusterId = "S" + i;
                double subClusterWeight = 0;
                for (Element e : nodesList) {
                    String nodeId = "N" + j;
                    String nodeIp = e.element("ip").getStringValue();
                    String nodePort = e.element("port").getStringValue();
                    double nodeWeight = Double.parseDouble(e.element("weight").getStringValue());
                    subClusterWeight += nodeWeight;
                    Cluster physicalNode = new PhysicalNode(nodeId, nodeIp, nodePort, subClusterId, 0, nodeWeight, true, 100);
                    clusterList.put(nodeId, physicalNode);
                    j++;
                }
                rootWeight += subClusterWeight;
                Cluster subCluster = new SubCluster(subClusterId, "", "", rootId, subClusterChildrenNumber, subClusterWeight, true);
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
                ClusterStructureMap clusterStructureMap = c.getCachedTreeStructure();
                clusterStructureMap.setEpoch(0);
                clusterStructureMap.setNumberOfReplicas(numberOfReplicas);

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

//            root.setCachedTreeStructure(fakeParentOfRoot.getCachedTreeStructure());
            return fakeParentOfRoot.getCachedTreeStructure();
        } catch (DocumentException e) {
            e.printStackTrace();
        }
        return null;
    }
}
