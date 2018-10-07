package dht.common;

public class Context {
	private int epoch;
	static private Context instance = null;
	
	private Context()
	{
		epoch = 0;
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
	
}
