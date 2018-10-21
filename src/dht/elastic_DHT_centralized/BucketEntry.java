package dht.elastic_DHT_centralized;

public class BucketEntry {
    private boolean lockOn = false;
    // Specifies how long each bucket will be locked during data transfer between nodes
    private String lockPeriod = "5s";

    public boolean isLockOn() {
        return lockOn;
    }

    public void setLockOn(boolean lockOn) {
        this.lockOn = lockOn;
    }

    public String getLockPeriod() {
        return lockPeriod;
    }

    public void setLockPeriod(String lockPeriod) {
        this.lockPeriod = lockPeriod;
    }
}
