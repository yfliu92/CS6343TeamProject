package dht.elastic_DHT_centralized;

import java.util.HashMap;
import java.util.List;

public class PhysicalNode {

    private String id;

    private String ip;

    private int port;

    private String status; // active, inactive, failed

    private TableWithEpoch table;

    private HashMap<Integer, BucketEntry> bucketList;

    public final static String STATUS_ACTIVE = "active";

    public final static String STATUS_INACTIVE = "inactive";

    public PhysicalNode() {
    }
    public PhysicalNode(String id, String ip, int port, String status){
        this.id = id;
        this.ip = ip;
        this.port = port;
        this.status = status;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public TableWithEpoch getTable() {
        return table;
    }

    public void setTable(TableWithEpoch table) {
        this.table = table;
    }

    public HashMap<Integer, BucketEntry> getBucketList() {
        return bucketList;
    }

    public void setBucketList(HashMap<Integer, BucketEntry> bucketList) {
        this.bucketList = bucketList;
    }





}

