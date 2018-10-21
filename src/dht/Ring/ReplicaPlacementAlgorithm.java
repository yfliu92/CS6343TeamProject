package dht.Ring;

import java.util.List;

public interface ReplicaPlacementAlgorithm {

    List<PhysicalNode> getReplicas(LookupTable table, Indexable node);

    List<PhysicalNode> getReplicas(LookupTable table, int hash);

}
