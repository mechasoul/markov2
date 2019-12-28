package my.cute.markov2.impl;

import java.io.File;
import java.util.concurrent.locks.ReentrantLock;

public final class ShardLoader {

	private final String id;
	private final String path;
	private final int depth;
	private final SaveType saveType;
	private final ReentrantLock loadLock = new ReentrantLock();
	
	ShardLoader(String i, String p, int d, SaveType save) {
		this.id = i;
		this.path = p;
		this.depth = d;
		this.saveType = save;
	}
	
	DatabaseShard createShard(String prefix) {
		DatabaseShard shard = new DatabaseShard(this.id, prefix, this.path, this.depth);
		return shard;
	}
	
	DatabaseShard createAndLoadShard(String prefix) {
		DatabaseShard shard = new DatabaseShard(this.id, prefix, this.path, this.depth);
		synchronized(this.loadLock) {
			shard.load(this.saveType);
		}
		return shard;
	}
	
	StartDatabaseShard createStartShard() {
		StartDatabaseShard shard = new StartDatabaseShard(this.id, MarkovDatabaseImpl.START_PREFIX, this.path);
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
	 * (ie, <prefix>.database)
	 */
	DatabaseShard getShardFromFile(File file) {
		//db files are saved as <prefix>.database, so this retrieves the prefix from the file
		String prefix = file.getName().split("\\.")[0];
		if(prefix.equals(MarkovDatabaseImpl.START_PREFIX)) {
			return this.loadStartShard(this.createStartShard());
		} else {
			return this.createAndLoadShard(prefix);
		}
	}
	
	ReentrantLock getLoadLock() {
		return this.loadLock;
	}
	
}
