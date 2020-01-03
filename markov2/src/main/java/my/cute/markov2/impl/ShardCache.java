package my.cute.markov2.impl;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;

import my.cute.markov2.exceptions.FollowingWordRemovalException;
import my.cute.markov2.exceptions.UncheckedFollowingWordRemovalException;

class ShardCache {
	
	@SuppressWarnings("unused")
	private static final Logger logger = LoggerFactory.getLogger(ShardCache.class);
	
	private final String id;
	private final LoadingCache<String, DatabaseShard> cache;
	private final int capacity;
	private final SaveType saveType;
	private final ShardLoader shardLoader;
	private final StartDatabaseShard startShard;
	private final int cleanupThreshold;
	private final boolean fixedCleanup;
	private final ReentrantLock saveLock = new ReentrantLock();
	
	private int cleanCount = 0;
	
	ShardCache(String i, int c, String path, SaveType save, Executor executorService, int cleanupThreshold) {
		this.id = i;
		this.capacity = c;
		this.saveType = save;
		this.shardLoader = new ShardLoader(this.id, path, this.saveType);
		this.cleanupThreshold = cleanupThreshold;
		this.fixedCleanup = this.cleanupThreshold > 0;
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
						synchronized(saveLock) {
							value.save(saveType);
						}
					}
				})
				//CacheLoader rule
				//i seriously think method reference notation is way less readable?
				.build(key -> this.createDatabaseShard(key));
		this.startShard = this.shardLoader.createStartShard();
	}
	
	
	
	DatabaseShard get(String key) {
		if(key.equals(MarkovDatabaseImpl.START_KEY)) return this.startShard;
		return this.cache.get(key);
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
		//note the use of == here so this will break if strings aren't intern'd
		if(key == MarkovDatabaseImpl.START_KEY) {
			//start shard always being loaded means concurrency problems w/
			//reloading shards are avoided so we can just call method directly
			this.startShard.addFollowingWord(bigram, followingWord);
		} else {
			//compute is always atomic
			this.cache.asMap().compute(key, (shardKey, shard) ->
			{
				//i dont like that i'm duplicating the cacheloader rule here
				if(shard == null) shard = createDatabaseShard(shardKey);
				shard.addFollowingWord(bigram, followingWord);
				return shard;
			});
		}
		
		this.checkFixedCleanup();
	}
	
	/*
	 * throws FollowingWordRemovalException if the given followingWord wasn't successfully 
	 * removed from the fws for the given bigram in the shard with the given key
	 * (exception propagated from DatabaseShard.removeFollowingWord(Bigram, String))
	 */
	void removeFollowingWord(String key, Bigram bigram, String followingWord) throws FollowingWordRemovalException {
		if(key == MarkovDatabaseImpl.START_KEY) {
			this.startShard.removeFollowingWord(bigram, followingWord);
		} else {
			try {
				/*
				 * need to use atomic compute to avoid concurrency issues which then 
				 * necessitates this awkward try-catch bs to get the thrown exception 
				 * out from the lambda so it can be thrown from this method 
				 */
				this.cache.asMap().compute(key, (shardKey, shard) ->
				{
					if(shard == null) shard = createDatabaseShard(shardKey);
					try {
						shard.removeFollowingWord(bigram, followingWord);
					} catch (FollowingWordRemovalException e) {
						//exception encountered. throw runtimeexception to catch it outside of lambda
						throw new UncheckedFollowingWordRemovalException(e);
					}
					return shard;
				});
			} catch (UncheckedFollowingWordRemovalException ex) {
				//rethrow the checked exception
				throw ex.getCause();
			}
		}
		
		this.checkFixedCleanup();
	}
	
//	/*
//	 * jank as hell
//	 * currently unused because no concurrency problems occur from calling contains() so 
//	 * we can just call it on the shard directly from MarkovDatabaseImpl.contains()
//	 */
//	boolean contains(String key, Bigram bigram, String followingWord, int count) {
//		if(key == MarkovDatabaseImpl.START_KEY) {
//			return this.startShard.contains(bigram, followingWord, count);
//		} else {
//			try {
//				this.cache.asMap().compute(key, (prefix, shard) ->
//				{
//					if(shard == null) shard = createDatabaseShard(prefix);
//					if(!shard.contains(bigram, followingWord, count)) {
//						throw new UncheckedFollowingWordRemovalException();
//					}
//					return shard;
//				});
//			} catch (UncheckedFollowingWordRemovalException ex) {
//				return false;
//			}
//			return true;
//		}
//	}
	
	StartDatabaseShard getStartShard() {
		return this.startShard;
	}
	
	private DatabaseShard createDatabaseShard(String key) {
		return this.shardLoader.createAndLoadShard(key);
	}
	
	void save() {
		this.cache.cleanUp();
		synchronized(this.saveLock) {
			for(Entry<String, DatabaseShard> entry : this.cache.asMap().entrySet()) {
				entry.getValue().save(this.saveType);
			}
			this.startShard.save(this.saveType);
		}
	}
	
	public void writeDatabaseShardString(File file, String path) throws IOException {
		this.shardLoader.getShardFromFile(file).writeDatabaseStringToFile(path);
	}
	
	private void checkFixedCleanup() {
		if(!this.fixedCleanup) return;
		this.cleanCount++;
		if(this.cleanCount >= this.cleanupThreshold) {
			this.cleanCount = 0;
			this.cache.cleanUp();
		}
	}
	
	void cleanUp() {
		this.cache.cleanUp();
	}
	
	/*
	 * prepare cache for use
	 */
	void load() {
		this.shardLoader.loadStartShard(this.startShard);
	}
	
	ReentrantLock getSaveLock() {
		return this.saveLock;
	}
	ReentrantLock getLoadLock() {
		return this.shardLoader.getLoadLock();
	}
	
}
