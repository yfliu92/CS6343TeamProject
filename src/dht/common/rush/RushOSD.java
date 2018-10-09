package dht.common.rush;

import java.util.Map;

public class RushOSD {
    private String name;
    private String ip;
    private int id;
    private long usedSize;
    private long totalSize;  // amount of bytes, e.g. 11 denotes 11 bytes
    private double weight;
    private Map<Integer, RushFileObject> data;
}
