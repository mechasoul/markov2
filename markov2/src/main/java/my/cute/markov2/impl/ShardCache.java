package my.cute.markov2.impl;

import java.util.ArrayDeque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class ShardCache {
	
	private class MyLinkedHashMap extends LinkedHashMap<String, DatabaseShard> {
		
		private static final long serialVersionUID = 1L;
		private final int maxCapacity;
		private final SaveType saveType;
		
		MyLinkedHashMap(int initialCapacity, float loadFactor, boolean accessMethod, int m, SaveType save) {
			super(initialCapacity, loadFactor, accessMethod);
			this.maxCapacity = m;
			this.saveType = save;
		}
		
		@Override
		protected boolean removeEldestEntry(Map.Entry<String, DatabaseShard> eldestEntry) {
			if(this.size() > this.maxCapacity) {
				eldestEntry.getValue().save(this.saveType);
				putSpareShard(eldestEntry.getValue());
				return true;
			} else {
				return false;
			}
		}
	}

	@SuppressWarnings("unused")
	private static final Logger logger = LoggerFactory.getLogger(ShardCache.class);
	
	private final String id;
	private final int capacity;
	private final MyLinkedHashMap cache;
	private final StartDatabaseShard startShard;
	private final SaveType saveType;
	private final Object cacheSizeLock = new Object();
	
	private DatabaseShard spareShard;
	private final BlockingQueue<DatabaseShard> spareShards = new ArrayBlockingQueue<DatabaseShard>(6);

	ShardCache(String i, int c, StartDatabaseShard s) {
		this.id = i;
		this.capacity = c;
		this.saveType = SaveType.SERIALIZE;
		this.cache = new MyLinkedHashMap(this.capacity * 4 / 3, 1f, true, this.capacity, this.saveType);
		this.startShard = s;
	}
	
	ShardCache(String i, int c, StartDatabaseShard s, SaveType save) {
		this.id = i;
		this.capacity = c;
		this.saveType = save;
		this.cache = new MyLinkedHashMap(this.capacity * 4 / 3, 1f, true, this.capacity, this.saveType);
		this.startShard = s;
	}
	
	/*
	 * looks for the shard corresponding to the given prefix in the cache
	 * returns the shard if it's found in the cache, otherwise returns null
	 */
	synchronized DatabaseShard get(String prefix) {
		//special case for start tokens (always kept in cache)
		if(prefix == MarkovDatabaseImpl.START_PREFIX) return this.startShard;
		
		return this.cache.get(prefix);
	}
	
	/*
	 * used when adding a shard to the cache
	 * returns the added shard for convenience (eg chaining)
	 */
	synchronized DatabaseShard add(DatabaseShard shard) {
		synchronized(this.cacheSizeLock) {
			this.cache.putIfAbsent(shard.getPrefix(), shard);
			return shard;
		}
	}
	
	synchronized DatabaseShard takeSpareShard() {
		DatabaseShard shard;
		while((shard = this.spareShard) == null) {
			try {
				this.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		this.spareShard = null;
		this.notifyAll();
		return shard;
		//return this.spareShards.take();
	}
	
	synchronized void putSpareShard(DatabaseShard shard) {
		while(this.spareShard != null) {
			try {
				this.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		this.spareShard = shard;
		this.notifyAll();
	}
	
	void save() {
		this.startShard.save(this.saveType);
		synchronized(this) {
			for(DatabaseShard shard : this.cache.values()) {
				shard.save(this.saveType);
			}
		}
	}
	
	boolean isFull() {
		synchronized(this.cacheSizeLock) {
			return this.cache.size() >= this.capacity;
		}
	}
	
	StartDatabaseShard getStartShard() {
		return this.startShard;
	}
	
	void initSpareShard(DatabaseShard shard) {
		if(this.spareShard != null || !this.cache.isEmpty()) 
			throw new IllegalStateException("ShardCache already initialized, can't load spare shard again");
	
		this.spareShard = shard;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("ShardCache [id=");
		builder.append(id);
		builder.append("]");
		return builder.toString();
	}
	
	
}
