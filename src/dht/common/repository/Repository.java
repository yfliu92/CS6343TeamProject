package dht.common.repository;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.oath.halodb.HaloDB;
import com.oath.halodb.HaloDBException;
import com.oath.halodb.HaloDBOptions;

import dht.common.Configuration;
import dht.common.FileObject;

public class Repository {
	private static Repository instance = null;
	private HaloDB db;
	
	private Repository() 
	{
		Configuration config = Configuration.getInstance();
		
		// Open a db with default options.
        HaloDBOptions options = new HaloDBOptions();

        // size of each data file will be 1GB.
        options.setMaxFileSize(1024 * 1024 * 1024);

        // the threshold at which page cache is synced to disk.
        // data will be durable only if it is flushed to disk, therefore
        // more data will be lost if this value is set too high. Setting
        // this value too low might interfere with read and write performance.
        options.setFlushDataSizeBytes(10 * 1024 * 1024);

        // The percentage of stale data in a data file at which the file will be compacted.
        // This value helps control write and space amplification. Increasing this value will
        // reduce write amplification but will increase space amplification.
        // This along with the compactionJobRate below is the most important setting
        // for tuning HaloDB performance. If this is set to x then write amplification 
        // will be approximately 1/x. 
        options.setCompactionThresholdPerFile(0.7);

        // Controls how fast the compaction job should run.
        // This is the amount of data which will be copied by the compaction thread per second.
        // Optimal value depends on the compactionThresholdPerFile option.
        options.setCompactionJobRate(50 * 1024 * 1024);

        // Setting this value is important as it helps to preallocate enough
        // memory for the off-heap cache. If the value is too low the db might
        // need to rehash the cache. For a db of size n set this value to 2*n.
        options.setNumberOfRecords(100_000_000);
        
        // Delete operation for a key will write a tombstone record to a tombstone file.
        // the tombstone record can be removed only when all previous version of that key
        // has been deleted by the compaction job.
        // enabling this option will delete during startup all tombstone records whose previous
        // versions were removed from the data file.
        options.setCleanUpTombstonesDuringOpen(true);

        // HaloDB does native memory allocation for the in-memory index.
        // Enabling this option will release all allocated memory back to the kernel when the db is closed.
        // This option is not necessary if the JVM is shutdown when the db is closed, as in that case
        // allocated memory is released automatically by the kernel.
        // If using in-memory index without memory pool this option,
        // depending on the number of records in the database,
        // could be a slow as we need to call _free_ for each record.
        options.setCleanUpInMemoryIndexOnClose(false);


        // ** settings for memory pool **
        options.setUseMemoryPool(true);

        // Hash table implementation in HaloDB is similar to that of ConcurrentHashMap in Java 7.
        // Hash table is divided into segments and each segment manages its own native memory.
        // The number of segments is twice the number of cores in the machine.
        // A segment's memory is further divided into chunks whose size can be configured here. 
        options.setMemoryPoolChunkSize(2 * 1024 * 1024);

        // using a memory pool requires us to declare the size of keys in advance.
        // Any write request with key length greater than the declared value will fail, but it
        // is still possible to store keys smaller than this declared size. 
        options.setFixedKeySize(4);


        // The directory will be created if it doesn't exist and all database files will be stored in this directory
        String directory = Integer.toString(config.getPort()) + "directory";

        // Open the database. Directory will be created if it doesn't exist.
        // If we are opening an existing database HaloDB needs to scan all the
        // index files to create the in-memory index, which, depending on the db size, might take a few minutes.
        try {
			this.db = HaloDB.open(directory, options);
		} catch (HaloDBException e) {
			System.err.println("Unable to create HaloDB repository: " + e.getLocalizedMessage());
		}
	}
	
	public void shutdown()
	{
		try {
			this.db.close();
		} catch (HaloDBException e) {
			e.printStackTrace();
		}
	}
	
	public static Repository getInstance()
	{
		if(instance == null)
			instance = new Repository();
		return instance;
	}
	
	public void saveFile(FileObject fo) throws HaloDBException
	{
		// TODO: Consider adding locking logic here but probably 
		try (ByteArrayOutputStream boas = new ByteArrayOutputStream() ) {
			ObjectOutputStream out = new ObjectOutputStream(boas);
			out.writeObject(fo);
			out.close();
			db.put(fo.getKeyByteArray() , boas.toByteArray());
		} catch (IOException e) {
			System.out.println("Repository saveFile IO Exception" + e.getLocalizedMessage());
		}
	}
	
	public FileObject getFile(int key) throws HaloDBException {
		byte[] keyBytes = FileObject.convertIntToByteArray(key);
		byte[] valueBytes = db.get(keyBytes);
		FileObject ret = null;
		
		try (ByteArrayInputStream boas = new ByteArrayInputStream(valueBytes) ) {
			ObjectInputStream in = new ObjectInputStream(boas);
			ret = (FileObject) in.readObject();
		} catch (IOException e) {
			System.err.println("Repository getFile IO Exception" + e.getLocalizedMessage());
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			System.err.println("Repository getFile Class Not Found Exception" + e.getLocalizedMessage());
		}
		
		return ret;
	}
}
