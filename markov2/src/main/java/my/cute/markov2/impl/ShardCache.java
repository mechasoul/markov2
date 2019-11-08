package my.cute.markov2.impl;

import java.io.File;
import java.util.Map.Entry;
import java.util.concurrent.Executor;

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
	
	ShardCache(String i, int c, int depth, String path, SaveType save, Executor executorService) {
		this.id = i;
		this.capacity = c;
		this.saveType = save;
		this.shardLoader = new ShardLoader(this.id, path, depth, this.saveType);
		this.cache = Caffeine.newBuilder()
				.maximumSize(this.capacity)
				.executor(executorService == null ? Runnable::run : executorService)
				.writer(new CacheWriter<String, DatabaseShard>() {
					@Override
					public void write(@NonNull String key, @NonNull DatabaseShard value) {
						//do nothing on entry load
					}

					//save db to disk on eviction
					@Override
					public void delete(@NonNull String key, @Nullable DatabaseShard value,
							@NonNull RemovalCause cause) {
						value.save(saveType);
					}
				})
				//CacheLoader rule
				//i seriously think method reference notation is way less readable?
				.build(prefix -> this.createDatabaseShard(prefix));
		this.startShard = this.shardLoader.loadStartShard(this.shardLoader.createStartShard());
//		(prefix, executor) -> 
//		CompletableFuture.supplyAsync(() -> this.shardLoader.createAndLoadShard(prefix), executor)
	}
	
	
	
	DatabaseShard get(String prefix) {
		if(prefix.equals(MarkovDatabaseImpl.START_PREFIX)) return this.startShard;
		return this.cache.get(prefix);
	}
	
	/*
	 * not totally happy with this method being here instead of just directly calling the shard
	 * but directly referring to the cache with cache.asMap().compute() lets us update
	 * map atomically which avoids concurrency problems (race condition stuff i guess?
	 * some words not getting processed randomly, must have to do with stale entries/
	 * trying to do stuff during eviction/idk but doing this seems to solve it)
	 * also see DatabaseShard.addFollowingWord()
	 */
	void addFollowingWord(String key, Bigram bigram, String followingWord) {
		//this.cache.cleanUp();
		if(key == MarkovDatabaseImpl.START_PREFIX) {
			//start shard always being loaded means concurrency problems w/
			//reloading shards are avoided so we can just call method directly
			this.startShard.addFollowingWord(bigram, followingWord);
		} else {
			//compute is always atomic
			this.cache.asMap().compute(key, (prefix, shard) ->
			{
				//i dont like that i'm duplicating the cacheloader rule here
				if(shard == null) shard = createDatabaseShard(prefix);
				shard.addFollowingWord(bigram, followingWord);
				return shard;
			});
		}
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
	
	@SuppressWarnings("unused")
	int getSize() {
		int count=0;
		for(Entry<String, DatabaseShard> entry : this.cache.asMap().entrySet()) {
			count++;
		}
		return count;
	}
	
	DatabaseShard getShardFromFile(File file) {
		return this.shardLoader.getShardFromFile(file);
	}
	
	void cleanUp() {
		this.cache.cleanUp();
	}
}
