package my.cute.markov2.impl;

import my.cute.markov2.MarkovDatabase;

public final class MarkovDatabaseBuilder {

	private String id;
	private String parentPath;
	
	//optional parameters
	private int depth = 1;
	private int shardCacheSize = 10;
	private SaveType saveType = SaveType.SERIALIZE;
	
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
}
