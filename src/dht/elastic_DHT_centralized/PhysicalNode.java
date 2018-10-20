package dht.elastic_DHT_centralized;

import java.util.LinkedList;

public class PhysicalNode {

    private String id;

    private String ip;

    private int port;

    private String status; // active, inactive

//    public final static String STATUS_ACTIVE = "active";
//
//    public final static String STATUS_INACTIVE = "inactive";

    public PhysicalNode() {
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
}

