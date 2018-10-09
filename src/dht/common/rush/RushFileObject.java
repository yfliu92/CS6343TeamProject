package dht.common.rush;

import dht.common.FileObject;
import dht.common.util.Murmur3;

public class RushFileObject extends FileObject {
    private int totalReplicas;
    private int replicaID;

    public RushFileObject() {
        super();
    }

    public RushFileObject(String filename, int version, int size, long lastModified, int totalReplicas, int replicaID) {
        super(filename, version, size, lastModified);
        this.totalReplicas = totalReplicas;
        this.replicaID = replicaID;
    }
}
