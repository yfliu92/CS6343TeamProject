package dht.common;

public class Context {
	private int epoch;
	private volatile boolean running;
	
	static private Context instance = null;
	
	private Context()
	{
		this.epoch = 0;
		this.running = true;
	}
	
	public static Context getInstance()
	{
		if(instance == null)
			instance = new Context();
		return instance;
	}
	
	public int getEpoch() {
		return this.epoch;
	}
	
	public void stopRunning() {
		this.running = false;
	}
	
	public boolean isRunning()
	{
		return this.running;
	}
	
}
