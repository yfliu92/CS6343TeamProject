package dht.common;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import dht.common.util.Murmur3;

public class FileObject implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public int key;
	public String filename;
	public int version;
	public int size;
	public long lastModified;
	
	public FileObject(String filename, int version, int size, long lastModified) {
		this.filename = filename;
		this.key = Murmur3.hash32(filename.getBytes());
		this.version = version;
		this.size = size;
		this.lastModified = lastModified;	
	}
	
	public void updateLastModified()
	{
		this.lastModified = System.currentTimeMillis();
	}
	
	public int getKey()
	{
		return this.key;
	}
	
	public byte[] getKeyByteArray()
	{
		return convertIntToByteArray(key);
	}
	
	public static byte[] convertIntToByteArray(int key)
	{
		return ByteBuffer.allocate(4).putInt(key).array();
	}
	
	public static FileObject getRandom()
	{
		String filename = UUID.randomUUID().toString().replace("-", "") + ".txt";
		int version = ThreadLocalRandom.current().nextInt(0, 100 + 1);
		int size = ThreadLocalRandom.current().nextInt(1000, 1000000 + 1);
		return new FileObject(filename, version, size, System.currentTimeMillis());
	}
}
