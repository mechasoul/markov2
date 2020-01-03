package my.cute.markov2.impl;

import java.io.File;
import java.util.concurrent.locks.ReentrantLock;

public final class ShardLoader {

	private final String id;
	private final String path;
	private final SaveType saveType;
	private final ReentrantLock loadLock = new ReentrantLock();
	
	ShardLoader(String i, String p, SaveType save) {
		this.id = i;
		this.path = p;
		this.saveType = save;
	}
	
	DatabaseShard createShard(String key) {
		DatabaseShard shard = new DatabaseShard(this.id, key, this.path);
		return shard;
	}
	
	DatabaseShard createAndLoadShard(String key) {
		DatabaseShard shard = new DatabaseShard(this.id, key, this.path);
		synchronized(this.loadLock) {
			shard.load(this.saveType);
		}
		return shard;
	}
	
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
	
	ReentrantLock getLoadLock() {
		return this.loadLock;
	}
	
}
