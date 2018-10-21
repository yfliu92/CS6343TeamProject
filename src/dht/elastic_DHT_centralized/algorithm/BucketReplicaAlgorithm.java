package dht.elastic_DHT_centralized.algorithm;

import dht.elastic_DHT_centralized.PhysicalNode;
import dht.elastic_DHT_centralized.TableWithEpoch;

import java.util.ArrayList;
import java.util.List;

public class BucketReplicaAlgorithm implements ReplicaPlacementAlgorithm {
    @Override
    public List<PhysicalNode> getReplicas(TableWithEpoch table, int hash) {
        List<String> replicaIDs = table.getTable().get(hash);
        ArrayList<PhysicalNode> replicas = new ArrayList<>();
        for (String ID : replicaIDs) {
            replicas.add(table.getPhysicalNodes().get(ID));
        }
        return replicas;
    }
}


