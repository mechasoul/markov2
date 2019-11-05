package my.cute.markov2.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

class DatabaseShard {

	private static final Logger logger = LoggerFactory.getLogger(DatabaseShard.class);
	protected static final Gson GSON = new GsonBuilder()
		.enableComplexMapKeySerialization()
		.create();
	protected static final Type DATABASE_TYPE = new TypeToken<ConcurrentHashMap<Bigram, FollowingWordSet>>() {}.getType();
	private static final SaveType DEFAULT_SAVE_TYPE = SaveType.JSON;
	
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
	protected ConcurrentMap<Bigram, FollowingWordSet> database;
	
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
		this.database = new ConcurrentHashMap<Bigram, FollowingWordSet>(6);
	}
	
	DatabaseShard(String id, String p, String parentPath) {
		this(id, p, parentPath, 0);
	}
	
	/*
	 * returns true if new entry in followingwordset was created as a result of this call
	 * maybe source of all the concurrency problems? this is a check-then-act race i think
	 * i feel like its fine though? but maybe with the addNewBigram() stuff some entries fall through
	 * not sure how it wasn't working properly when we were synchronizing on prefix locks tho? whatever
	 */
	boolean addFollowingWord(Bigram bigram, String followingWord) {
		FollowingWordSet followingWordSet = this.database.get(bigram);
		if(followingWordSet != null) {
			followingWordSet.addWord(followingWord);
			return false;
		} else {
			return this.addNewBigram(bigram, followingWord);
		}
	}
	
	private boolean addNewBigram(Bigram bigram, String followingWord) {
		return this.database.putIfAbsent(bigram, new FollowingWordSet(followingWord)) == null;
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
			} catch (IOException | ClassNotFoundException e) {
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
		String json = new String(Files.readAllBytes(this.path), StandardCharsets.UTF_8);
		this.database = GSON.fromJson(json, DATABASE_TYPE); 
		if(this.database == null) System.out.println(json);
	}
	
	void saveAsObject() throws IOException {
		FileOutputStream fileOutputStream = new FileOutputStream(this.path.toString());
		ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
		objectOutputStream.writeObject(this.database);
		objectOutputStream.close();	
	}

	@SuppressWarnings("unchecked")
	void loadFromObject() throws FileNotFoundException, IOException, ClassNotFoundException {
			FileInputStream fileInputStream = new FileInputStream(this.path.toString());
			ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
			this.database = (ConcurrentMap<Bigram, FollowingWordSet>) objectInputStream.readObject();
			objectInputStream.close();
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
		if(depth == 0) return parentPath + "/" + this.prefix + ".database";
		StringBuilder sb = new StringBuilder(parentPath);
		sb.append("/");
		int index = 0;
		while(index < depth) {
			sb.append(this.prefix.charAt(index));
			sb.append("/");
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
	 * maybe this shouuld just be that
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
			sb.append(bigramEntry.getValue().getTotalWordCount());
			for(Map.Entry<String, Integer> wordEntry : bigramEntry.getValue()) {
				sb.append(", ");
				sb.append("(");
				sb.append(wordEntry.getKey());
				sb.append(", ");
				sb.append(wordEntry.getValue());
				sb.append(")");
			}
			sb.append("}\r\n");
		}
		return sb.toString();
	}

	String getPrefix() {
		return prefix;
	}
}
