package my.cute.markov2.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.nustaq.serialization.FSTConfiguration;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import my.cute.markov2.exceptions.FollowingWordRemovalException;

class DatabaseShard {

	private static final Logger logger = LoggerFactory.getLogger(DatabaseShard.class);
	protected static final Gson GSON = new GsonBuilder()
		.enableComplexMapKeySerialization()
		.create();
	protected static final Type DATABASE_TYPE = new TypeToken<DatabaseWrapper>() {}.getType();
	private static final SaveType DEFAULT_SAVE_TYPE = SaveType.JSON;
	private static final int LARGE_WORD_SET_THRESHOLD = 24;
	
	protected static final FSTConfiguration CONF = FSTConfiguration.getDefaultConfiguration();
	
	static {
    	CONF.registerClass(ConcurrentHashMap.class, Bigram.class, String.class, DatabaseWrapper.class, SmallFollowingWordSet.class, LargeFollowingWordSet.class);
    	CONF.registerSerializer(Bigram.class, new Bigram.Serializer(), true);
    	CONF.registerSerializer(DatabaseWrapper.class, new DatabaseWrapper.Serializer(), true);
    	CONF.registerSerializer(SmallFollowingWordSet.class, new FollowingWordSet.Serializer(), true);
    	CONF.registerSerializer(LargeFollowingWordSet.class, new FollowingWordSet.Serializer(), true);
	}
	
	protected final String parentDatabaseId;
	/*
	 * prefix for this db shard
	 * prefixes represent all possible word prefixes of length equal to the database's depth
	 * eg for depth 1 A, B, C, ..., Z
	 * depth 2 AA, AB, ..., AZ, BA, BB, BC, ..., ZY, ZZ
	 * prefix length always matches depth exactly. words with less letters than prefix length
	 * go in the earliest shard matching the word, filling remainder with A's (eg the word "a" 
	 * in a db of depth 2 would go in shard AA, the word "by" in a db of depth 3 would go in 
	 * shard BYA)
	 */
	protected String prefix;
	protected Path path;
	/*
	 * database maps bigram to a followingwordset representing the words following that bigram
	 * starts as a light arraylist-based implementation and switches to a hashmap-based one
	 * once the followingwordset reaches a certain size
	 * goal is to minimize memory use as much as possible, sacrificing speed if necessary (to a point...)
	 */
	protected DatabaseWrapper database;
	
	DatabaseShard(String id, String p, String parentPath, int depth) {
		this.parentDatabaseId = id;
		this.prefix = p;
		String pathString = this.determinePath(parentPath, depth);
		try {
			FileUtils.forceMkdirParent(new File(pathString));
		} catch (IOException e) {
			logger.error("IOException on creating parent directory for shard " + this.toString() + ": " + e.getLocalizedMessage());
			e.printStackTrace();
		}
		this.path = Paths.get(pathString);
		this.database = new DatabaseWrapper(this.prefix, this.parentDatabaseId);
	}
	
	DatabaseShard(String id, String p, String parentPath) {
		this(id, p, parentPath, 0);
	}
	
	/*
	 * returns true if new entry in followingwordset was created as a result of this call
	 * is only used in atomic compute() context
	 * so the concurrency issues here (eg replacing FollowingWordSet) shouldnt actually be issues
	 * & consequently be careful using this if not synchronizing or w/e
	 */
	boolean addFollowingWord(Bigram bigram, String followingWord) {
		FollowingWordSet followingWordSet = this.database.get(bigram);
		if(followingWordSet != null) {
			followingWordSet.addWord(followingWord);
			//check for replacing small set with large
			//better way to do this?
			if(followingWordSet.size() >= LARGE_WORD_SET_THRESHOLD && followingWordSet instanceof SmallFollowingWordSet) {
				this.database.put(bigram, new LargeFollowingWordSet((SmallFollowingWordSet)followingWordSet));
			}
			return false;
		} else {
			return this.addNewBigram(bigram, followingWord);
		}
	}
	
	//start with smallfollowingwordset
	private boolean addNewBigram(Bigram bigram, String followingWord) {
		return this.database.putIfAbsent(bigram, new SmallFollowingWordSet(followingWord, bigram, this.parentDatabaseId)) == null;
	}
	
	/*
	 * gets a weighted random word that follows the given bigram according to the shard
	 * throws IllegalArgumentException if the given bigram isn't present in the shard
	 * (shouldn't happen)
	 */
	String getFollowingWord(Bigram bigram) throws IllegalArgumentException {
		FollowingWordSet followingWordSet = this.database.get(bigram);
		if(followingWordSet == null) throw new IllegalArgumentException(bigram.toString() + " not found in " + this.toString());
		
		return followingWordSet.getRandomWeightedWord();
	}
	
	/*
	 * checks for existence of the given followingword for the given bigram
	 * returns true if the bigram exists in the database and the given followingword
	 * has been recorded for that bigram at least once, and false otherwise (the given
	 * followingword has never been used for the given bigram, or the given bigram has 
	 * never been used)
	 */
	boolean contains(Bigram bigram, String followingWord) {
		FollowingWordSet followingWordSet = this.database.get(bigram);
		if(followingWordSet == null) return false;
		
		return followingWordSet.contains(followingWord);
	}
	
	boolean contains(Bigram bigram, String followingWord, int count) {
		FollowingWordSet followingWordSet = this.database.get(bigram);
		if(followingWordSet == null) return false;
		
		return followingWordSet.contains(followingWord, count);
	}
	
	/*
	 * similar to addFollowingWord(Bigram, String), there are concurrency problems
	 * here if this isnt done in an atomic context
	 * note this method should only be called if the given followingWord is known 
	 * to exist in the followingwordset for the given bigram
	 * throws FollowingWordRemovalException if the bigram is not found in the db,
	 * 		or if the given word is not found in the fws for the given bigram
	 */
	void removeFollowingWord(Bigram bigram, String followingWord) throws FollowingWordRemovalException {
		FollowingWordSet followingWordSet = this.database.get(bigram);
		if(followingWordSet == null) throw new FollowingWordRemovalException("illegal attempt to remove word '" 
				+ followingWord + "' from fws for bigram " + bigram + " in " + this + ": fws not found for given bigram");
		
		if(followingWordSet.remove(followingWord)) {
			if(followingWordSet.isEmpty()) {
				this.remove(bigram);
			}
		} else {
			//remove was unsuccessful, so structure of shard is probably not what was expected
			throw new FollowingWordRemovalException("illegal attempt to remove word '" + followingWord + "' from fws for bigram "
					+ bigram + " in " + this + ": word not found");
		}
	}
	
	boolean remove(Bigram bigram) {
		return this.database.remove(bigram);
	}
	
	void save() {
		this.save(DEFAULT_SAVE_TYPE);
	}
	
	/*
	 * used to save shard to disk
	 * defers to either saving as raw text or serializing 
	 * consider implementing some other solution for serializing here (protobuf?)
	 * should be fine for now though since this project is entirely localized
	 * returns true if save successful, false if exception encountered
	 */
	void save(SaveType saveType) {
		if(saveType == SaveType.JSON) {
			try {
				this.saveAsText();
			} catch (IOException e) {
				logger.error("couldn't save (json) " + this.toString() + ": " + e.getLocalizedMessage());
				e.printStackTrace();
			}
		} else {
			try {
				this.saveAsObject();
			} catch (IOException e) {
				logger.error("couldn't save (serialize) " + this.toString() + ": " + e.getLocalizedMessage());
				e.printStackTrace();
			}
		}
	}
	
	void load() {
		this.load(DEFAULT_SAVE_TYPE);
	}
	
	void load(SaveType saveType) {
		if(saveType == SaveType.JSON) {
			try {
				this.loadFromText();
			} catch (FileNotFoundException | NoSuchFileException e) {
				//logger.info("couldn't load (json) " + this.toString() + ", file not found (first load?) ex: " + e.getLocalizedMessage());
			} catch (IOException e) {
				logger.error("couldn't load (json) " + this.toString() + ": " + e.getLocalizedMessage());
				e.printStackTrace();
			}
		} else {
			try {
				this.loadFromObject();
			} catch (FileNotFoundException e) {
				//logger.info("couldn't load (deserialize) " + this.toString() + ", file not found (first load?) ex: " + e.getLocalizedMessage());
			} catch (Exception e) {
				logger.error("couldn't load (deserialize) " + this.toString() + ": " + e.getLocalizedMessage());
				e.printStackTrace();
			} 
		}
	}
	
	void saveAsText() throws IOException {
		String json = GSON.toJson(this.database, DATABASE_TYPE);
		Files.write(this.path, json.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
	}
	
	void loadFromText() throws FileNotFoundException, NoSuchFileException, IOException {
		try (BufferedReader reader = Files.newBufferedReader(this.path, StandardCharsets.UTF_8)) {
			this.database = GSON.fromJson(reader.readLine(), DATABASE_TYPE); 
		}
	}
	
	void saveAsObject() throws IOException {
		FileOutputStream fileOutputStream = new FileOutputStream(this.path.toString());
		FSTObjectOutput out = CONF.getObjectOutput(fileOutputStream);
		out.writeObject(this.database, DatabaseWrapper.class);
		out.flush();
		fileOutputStream.close();
	}

	void loadFromObject() throws Exception {
		FileInputStream fileInputStream = new FileInputStream(this.path.toString());
		FSTObjectInput in = CONF.getObjectInput(fileInputStream);
		this.database = (DatabaseWrapper) in.readObject(DatabaseWrapper.class);
		fileInputStream.close();
	}
	
	/*
	 * obtain the path for the file representing this shard on the local disk
	 * paths are separated by letter according to depth
	 * eg a database with depth 1 has shards representing each letter of the alphabet
	 * and their corresponding prefixes would be A, B, C, ..., Z
	 * depth 2 would be AA, AB, ..., AZ, BA, BB, ..., ZY, ZZ
	 * the local file path for each shard would be ../A/A/AA.database, ../A/B/AB.database,
	 * ..., ../B/A/BA.database, ..., ../Z/Z/ZZ.database
	 * words that don't have as many letters as the prefix go in the first db starting with
	 * as many letters as the word has, remaining letters filled with A
	 * eg the word "a" in a db of depth 2 would reside in AA.database
	 * additionally there's one shard for numbers (prefix 0), one for punctuation (as 
	 * specified by matching against regex \\p{Punct}, prefix !), and one for other (prefix
	 * @)
	 */
	private String determinePath(String parentPath, int depth) {
		//special shard path for zero depth database
		if(depth == 0) return parentPath + File.separator + this.prefix + ".database";
		StringBuilder sb = new StringBuilder(parentPath);
		sb.append(File.separator);
		int index = 0;
		while(index < depth) {
			sb.append(this.prefix.charAt(index));
			sb.append(File.separator);
			index++;
		}
		sb.append(this.prefix);
		sb.append(".database");
		return sb.toString();
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("DatabaseShard [parentDatabaseId=");
		builder.append(parentDatabaseId);
		builder.append(", prefix=");
		builder.append(prefix);
		builder.append("]");
		return builder.toString();
	}
	
	String toStringFull() {
		StringBuilder builder = new StringBuilder();
		builder.append("DatabaseShard [parentDatabaseId=");
		builder.append(parentDatabaseId);
		builder.append(", prefix=");
		builder.append(prefix);
		builder.append(", words=");
		builder.append(this.database);
		builder.append("]");
		return builder.toString();
	}
	
	/*
	 * more human readable toString() basically
	 * maybe this should just be that
	 */
	String getDatabaseString() {
		StringBuilder sb = new StringBuilder();
		for(Map.Entry<Bigram, FollowingWordSet> bigramEntry : this.database.entrySet()) {
			sb.append("(");
			sb.append(bigramEntry.getKey().getWord1());
			sb.append(", ");
			sb.append(bigramEntry.getKey().getWord2());
			sb.append(") -> {");
			sb.append("count=");
			sb.append(bigramEntry.getValue().size());
			sb.append(", ");
			sb.append(bigramEntry.getValue().toStringPlain());
			sb.append("}\r\n");
		}
		return sb.toString();
	}
	
	void writeDatabaseStringToFile(String filePath) throws IOException {
		Path path = Paths.get(filePath);
		StringBuilder sb = new StringBuilder();
		int count=0;
		for(Map.Entry<Bigram, FollowingWordSet> bigramEntry : this.database.entrySet()) {
			sb.append("(");
			sb.append(bigramEntry.getKey().getWord1());
			sb.append(", ");
			sb.append(bigramEntry.getKey().getWord2());
			sb.append(") -> {");
			sb.append("count=");
			sb.append(bigramEntry.getValue().size());
			sb.append(", ");
			sb.append(bigramEntry.getValue().toStringPlain());
			sb.append("}\r\n");
			count++;
			if(count >= 1000) {
				Files.write(path, sb.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
				sb = new StringBuilder();
				count = 0;
			}
		}
	}

	String getPrefix() {
		return prefix;
	}

	
}
