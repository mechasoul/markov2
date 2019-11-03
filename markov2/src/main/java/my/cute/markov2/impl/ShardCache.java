package my.cute.markov2.impl;

import java.io.File;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;

class ShardCache {
	
	@SuppressWarnings("unused")
	private static final Logger logger = LoggerFactory.getLogger(ShardCache.class);
	
	private final String id;
	private LoadingCache<String, DatabaseShard> cache;
	private final int capacity;
	private final SaveType saveType;
	private final ShardLoader shardLoader;
	private final StartDatabaseShard startShard;
	private final ExecutorService executor;
	
	ShardCache(String i, int c, int depth, String path, SaveType save, ExecutorService executorService) {
		this.id = i;
		this.capacity = c;
		this.saveType = save;
		this.shardLoader = new ShardLoader(this.id, path, depth, this.saveType);
		this.executor = executorService;
		this.cache = Caffeine.newBuilder()
				.maximumSize(this.capacity)
				.executor(Runnable::run)
				.writer(new CacheWriter<String, DatabaseShard>() {
					@Override
					public void write(@NonNull String key, @NonNull DatabaseShard value) {
						//do nothing on entry load
					}

					@Override
					public void delete(@NonNull String key, @Nullable DatabaseShard value,
							@NonNull RemovalCause cause) {
						value.save(saveType);
					}
				})
				.build(prefix -> createDatabaseShard(prefix));
		this.startShard = this.shardLoader.loadStartShard(this.shardLoader.createStartShard());
//		(prefix, executor) -> 
//		CompletableFuture.supplyAsync(() -> this.shardLoader.createAndLoadShard(prefix), executor)
	}
	
	DatabaseShard get(String prefix) {
		if(prefix.equals(MarkovDatabaseImpl.START_PREFIX)) return this.startShard;
		return this.cache.get(prefix);
	}
	
	StartDatabaseShard getStartShard() {
		return this.startShard;
	}
	
	private DatabaseShard createDatabaseShard(String prefix) {
		return this.shardLoader.createAndLoadShard(prefix);
	}
	
	void save() {
		this.cache.cleanUp();
		for(Entry<String, DatabaseShard> entry : this.cache.asMap().entrySet()) {
			entry.getValue().save(this.saveType);
		}
		this.startShard.save(this.saveType);
	}
	
	String getDatabaseString(File file) {
		return this.shardLoader.getShardFromFile(file).getDatabaseString();
	}
}
