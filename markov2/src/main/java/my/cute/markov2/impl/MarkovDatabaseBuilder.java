package my.cute.markov2.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

import my.cute.markov2.MarkovDatabase;

/*
 * entry point from outside this api
 * used to construct a MarkovDatabase object
 */
public final class MarkovDatabaseBuilder {

	/*
	 * unique identifier for this database
	 * changing id eg in between runs will effectively create a new database, 
	 * since id is used in directory file structure as well as identification
	 * in program
	 */
	private final String id;
	/*
	 * parent directory for this database to create its save directory in
	 */
	private final String parentPath;
	
	//optional parameters
	/*
	 * size of cache (number of shards kept in memory at once)
	 * cache will temporarily go over this number until it calls cleanUp()
	 * setting a low fixedCleanupThreshold will reduce this, but requires more
	 * frequent saves/loads (slower) (see fixedCleanupThreshold)
	 * a size of 0 will cause entries to be evicted immediately after entering cache
	 * a negative size (default) will impose no size restriction on cache
	 */
	private int shardCacheSize = -1;

	/*
	 * specifies Executor used by the cache for additional tasks (eg maintenance)
	 * passing in null will cause the database to execute all tasks in the current thread,
	 * effectively disabling all cache-related multithreading
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
