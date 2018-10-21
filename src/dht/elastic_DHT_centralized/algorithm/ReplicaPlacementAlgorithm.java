package dht.elastic_DHT_centralized.algorithm;

import dht.elastic_DHT_centralized.PhysicalNode;
import dht.elastic_DHT_centralized.TableWithEpoch;

import java.util.List;

public interface ReplicaPlacementAlgorithm {
    //List<PhysicalNode> getReplicas(TableWithEpoch table, Indexable node);
    List<PhysicalNode> getReplicas(TableWithEpoch table, int hash);
}
