package my.cute.markov2.impl;

import java.io.File;

/*
 * responsible for loading databaseshards from disk, so it can pass them
 * to the relevant shardcache. each shardcache uses one shardloader.
 */
public final class ShardLoader {

	/*
	 * id of parent database
	 */
	private final String id;
	/*
	 * database directory path for this database
	 */
	private final String path;
	/*
	 * method of deserialization that should be used
	 */
	private final SaveType saveType;
	/*
	 * used to lock loading operations to prevent concurrency issues with
	 * potential competitors (especially backup operations, since db state
	 * could be inconsistent if loads/saves happen during backup load/save)
	 * should probably be a ReentrantLock or something but i dunno what i'm
	 * really doing with the concurrency stuff yet
	 */
	private final Object loadLock = new Object();
	
	ShardLoader(String i, String p, SaveType save) {
		this.id = i;
		this.path = p;
		this.saveType = save;
	}
	
	/*
	 * creates a shard object for the given key. shard will contain no data
	 */
	DatabaseShard createShard(String key) {
		DatabaseShard shard = new DatabaseShard(this.id, key, this.path);
		return shard;
	}
	
	/*
	 * creates a shard object for the given key and loads all data from the
	 * relevant file. shard will contain all data recorded for it in the db
	 * 
	 * TODO should lock on loadLock before calling load?
	 */
	DatabaseShard createAndLoadShard(String key) {
		DatabaseShard shard = new DatabaseShard(this.id, key, this.path);
		shard.load(this.saveType);
		return shard;
	}
	
	/*
	 * creates the special shard used for start-of-line bigrams
	 * this will in applications almost certainly be the heaviest shard
	 * by a significant amount, so create and load are separated to
	 * allow the start shard to be initialized at some later time if desired
	 * like with all shards, should be loaded before any operations using
	 * shard state (eg adds, removes, contains) are performed
	 */
	StartDatabaseShard createStartShard() {
		StartDatabaseShard shard = new StartDatabaseShard(this.id, MarkovDatabaseImpl.START_KEY, this.path);
		return shard;
	}
	
	StartDatabaseShard loadStartShard(StartDatabaseShard shard) {
		synchronized(this.loadLock) {
			shard.load(this.saveType);
		}
		return shard;
	}
	
	/*
	 * creates and loads a shard whose database is represented by the given file
	 * it's assumed that files passed in are correct database files w/ correct name structure
	 * (ie, <key>.database)
	 */
	DatabaseShard getShardFromFile(File file) {
		//db files are saved as <key>.database, so this retrieves the key from the file
		String key = file.getName().split("\\.")[0];
		if(key.equals(MarkovDatabaseImpl.START_KEY)) {
			return this.loadStartShard(this.createStartShard());
		} else {
			return this.createAndLoadShard(key);
		}
	}
	
	Object getLoadLock() {
		return this.loadLock;
	}
	
}
