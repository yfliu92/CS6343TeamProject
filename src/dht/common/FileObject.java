package dht.common;

import java.io.Serializable;

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
	
	public FileObject(String filename, int version, int size, int lastModified) {
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
}
