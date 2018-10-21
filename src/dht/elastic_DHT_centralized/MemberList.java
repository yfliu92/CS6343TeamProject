package dht.elastic_DHT_centralized;

import java.util.HashMap;
import java.util.List;

public class MemberList {
    private HashMap<Integer, PhysicalNode> members;
    private PhysicalNode proxy;

    public HashMap<Integer, PhysicalNode> getMembers() {
        return members;
    }

    public void setMembers(HashMap<Integer, PhysicalNode> members) {
        this.members = members;
    }

    public PhysicalNode getProxy() {
        return proxy;
    }

    public void setProxy(PhysicalNode proxy) {
        this.proxy = proxy;
    }

    public void add(PhysicalNode node){

    }
    public void add(List<PhysicalNode> nodes){

    }
    public void remove(PhysicalNode node){

    }
    public void remove(List<PhysicalNode> nodes){

    }
}
