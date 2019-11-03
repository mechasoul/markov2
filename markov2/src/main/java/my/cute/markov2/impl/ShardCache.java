package my.cute.markov2.impl;

import java.io.File;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;

class ShardCache {
	
	@SuppressWarnings("unused")
	private static final Logger logger = LoggerFactory.getLogger(ShardCache.class);
	
	private final String id;
	private AsyncLoadingCache<String, DatabaseShard> cache;
	private final int capacity;
	private final SaveType saveType;
	private final ShardLoader shardLoader;
	private final StartDatabaseShard startShard;
	
	ShardCache(String i, int c, int depth, String path, SaveType save) {
		this.id = i;
		this.capacity = c;
		this.saveType = save;
		this.shardLoader = new ShardLoader(this.id, path, depth, this.saveType);
		this.cache = Caffeine.newBuilder()
				.maximumSize(this.capacity)
				.<String, DatabaseShard>removalListener((prefix, shard, cause) -> shard.save(this.saveType))
				.buildAsync((prefix, executor) -> createDatabaseShardAsync(prefix, executor));
		this.startShard = this.shardLoader.loadStartShard(this.shardLoader.createStartShard());
//		(prefix, executor) -> 
//		CompletableFuture.supplyAsync(() -> this.shardLoader.createAndLoadShard(prefix), executor)
	}
	
	CompletableFuture<DatabaseShard> get(String prefix) {
		if(prefix.equals(MarkovDatabaseImpl.START_PREFIX)) return CompletableFuture.completedFuture(this.startShard);
		return this.cache.get(prefix);
	}
	
	StartDatabaseShard getStartShard() {
		return this.startShard;
	}
	
	private CompletableFuture<DatabaseShard> createDatabaseShardAsync(String prefix, Executor executor) {
		return CompletableFuture.supplyAsync(() -> this.shardLoader.createAndLoadShard(prefix), executor);
	}
	
	void save() {
		for(Entry<String, CompletableFuture<DatabaseShard>> entry : this.cache.asMap().entrySet()) {
			entry.getValue().thenAcceptAsync(shard -> shard.save(this.saveType));
		}
		this.startShard.save(this.saveType);
	}
	
	String getDatabaseString(File file) {
		return this.shardLoader.getShardFromFile(file).getDatabaseString();
	}
}
