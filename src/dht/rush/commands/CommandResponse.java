package dht.rush.commands;

import dht.rush.clusters.Cluster;

import java.util.HashMap;
import java.util.Map;

public class CommandResponse {
    private int status;
    // key: from node, value: pgid and new destination cluster
    private Map<String, Cluster[]> transferMap = null;

    public CommandResponse() {
        this.status = -1;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public Map<String, Cluster[]> getTransferMap() {
        return transferMap;
    }

    public void setTransferMap(Map<String, Cluster[]> transferMap) {
        this.transferMap = transferMap;
    }

    /**
     * Only put the elements in the transferMap into the return string.
     *
     * @return
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (transferMap != null && transferMap.size() > 0) {
            for (Map.Entry<String, Cluster[]> entry : transferMap.entrySet()) {

                Cluster[] clusters = entry.getValue();
                sb.append("[");

                sb.append("PG_id: " + entry.getKey() + ", ");

                sb.append("From: " + clusters[0].toString() + ", ");

                sb.append("To: " + clusters[1].toString());

                sb.append("]");
            }
        }
        return sb.toString();
    }
}
