package my.cute.markov2.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

import my.cute.markov2.MarkovDatabase;

public final class MarkovDatabaseBuilder {

	/*
	 * unique identifier for this database
	 * if it changes, database will create a new directory and not load old versions
	 */
	private String id;
	/*
	 * parent directory for this database to create its save directory in
	 */
	private String parentPath;
	
	//optional parameters
	/*
	 * depth of database. number of shards is equal to ~29^depth
	 */
	private int depth = 1;
	/*
	 * size of cache (number of shards kept in memory at once)
	 * cache will temporarily go over this number until it calls cleanUp()
	 * setting a low fixedCleanupThreshold will reduce this, but requires more
	 * frequent saves/loads (slower)
	 */
	private int shardCacheSize = 10;
	/*
	 * method of serialization. TODO remove this and just use serialize, its quicker than json
	 */
	private SaveType saveType = SaveType.SERIALIZE;
	/*
	 * specifies Executor used by the cache for additional tasks (eg maintenance)
	 */
	private ExecutorService executorService = ForkJoinPool.commonPool();
	/*
	 * database will call cleanUp() to evict & save stale entries every fixedCleanupThreshold operations
	 * setting this to a low value (eg ~1000 or especially lower) will make the database more strictly
	 * follow its set shardCacheSize and reduce memory use, but will impose more frequent save/load 
	 * operations and consequently make things slower
	 * nonnegative. set to 0 to disable fixed cleanup and let the cache decide when to do it (default)
	 */
	private int fixedCleanupThreshold = 0;
	
	public MarkovDatabaseBuilder(String id, String parentPath) {
		this.id = id;
		this.parentPath = parentPath;
		
	}
	
	public MarkovDatabaseBuilder depth(int depth) {
		this.depth = depth;
		return this;
	}
	
	public MarkovDatabaseBuilder saveType(SaveType saveType) {
		this.saveType = saveType;
		return this;
	}
	
	public MarkovDatabaseBuilder shardCacheSize(int shardCacheSize) {
		this.shardCacheSize = shardCacheSize;
		return this;
	}
	
	public MarkovDatabaseBuilder executorService(ExecutorService executor) {
		this.executorService = executor;
		return this;
	}
	
	public MarkovDatabaseBuilder fixedCleanupThreshold(int threshold) {
		if(threshold < 0) throw new IllegalArgumentException("fixedCleanupThreshold must be nonnegative");
		this.fixedCleanupThreshold = threshold;
		return this;
	}
	
	public MarkovDatabase build() {
		return new MarkovDatabaseImpl(this);
	}

	public String getId() {
		return id;
	}

	public String getParentPath() {
		return parentPath;
	}

	public int getDepth() {
		return depth;
	}

	public SaveType getSaveType() {
		return saveType;
	}
	
	public int getShardCacheSize() {
		return shardCacheSize;
	}
	
	public ExecutorService getExecutorService() {
		return executorService;
	}
	
	public int getFixedCleanupThreshold() {
		return fixedCleanupThreshold;
	}
}
