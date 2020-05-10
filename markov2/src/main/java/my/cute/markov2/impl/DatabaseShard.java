package my.cute.markov2.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.nustaq.serialization.FSTConfiguration;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import my.cute.markov2.exceptions.FollowingWordRemovalException;

/*
 * class representing a part of the database
 * each shard contains all data for a given key, in the form of bigrams matching
 * that key and all the words seen to have followed a given bigram and their frequencies
 */
class DatabaseShard {

	private static final Logger logger = LoggerFactory.getLogger(DatabaseShard.class);
	
	//gson fields. currently unused
	protected static final Gson GSON = new GsonBuilder()
		.enableComplexMapKeySerialization()
		.create();
	protected static final Type DATABASE_TYPE = new TypeToken<DatabaseWrapper>() {}.getType();
	//end gson fields
	
	private static final SaveType DEFAULT_SAVE_TYPE = SaveType.SERIALIZE;
	/*
	 * these control which FollowingWordSet implementation is used at different database sizes
	 * TinyFollowingWordSet is used until the set contains at least SMALL_WORD_SET_THRESHOLD
	 * entries, at which point it's converted to a SmallFollowingWordSet, which is converted to
	 * LargeFollowingWordSet at LARGE_WORD_SET_THRESHOLD entries
	 */
	private static final int LARGE_WORD_SET_THRESHOLD = 24;
	private static final int SMALL_WORD_SET_THRESHOLD = 4;
	
	protected static final FSTConfiguration CONF = FSTConfiguration.getDefaultConfiguration();
	
	static {
    	CONF.registerClass(ConcurrentHashMap.class, Bigram.class, String.class, DatabaseWrapper.class, SmallFollowingWordSet.class, 
    			LargeFollowingWordSet.class, TinyFollowingWordSet.class);
    	CONF.registerSerializer(Bigram.class, new Bigram.Serializer(), true);
    	CONF.registerSerializer(DatabaseWrapper.class, new DatabaseWrapper.Serializer(), true);
    	CONF.registerSerializer(SmallFollowingWordSet.class, new FollowingWordSet.Serializer(), true);
    	CONF.registerSerializer(LargeFollowingWordSet.class, new FollowingWordSet.Serializer(), true);
    	CONF.registerSerializer(TinyFollowingWordSet.class, new FollowingWordSet.Serializer(), true);
	}
	
	protected final String parentDatabaseId;
	/*
	 * key for this db shard
	 * keys are special strings that represent the bigrams used in that part of the database
	 * keys and databaseshards are 1-to-1; every bigram that maps to a given key has its 
	 * corresponding data (ie, followingwordset) held in the shard that corresponds to that key
	 * key chars are ascii letters, 0 for numbers, ! for punctuation, @ for other, and ~
	 * used to represent the space between word1 and word2 in the bigram
	 * each word has up to MarkovDatabaseImpl.MAX_CHARS_PER_KEY_WORD chars representing it in
	 * the key, so max key length is 2 * MAX_CHARS_PER_KEY_WORD + 1
	 * eg bigram (im, gay), MAX_CHARS_PER_KEY_WORD=3 has key "IM~GAY"
	 * bigram (999, .things), MAX_CHARS_PER_KEY_WORD=2 has key "00~!T"
	 * bigram (abcdefghij, hellohowareyoutoday), MAX_CHARS_PER_KEY_WORD=10 has key "ABCDEFGHIJ~HELLOHOWAR"
	 */
	protected String key;
	/*
	 * the path to the file on disk that holds this shard's data
	 * to avoid dumping all our shard files in a single directory, paths are split by each 
	 * character in the shard's key, up to MarkovDatabaseImpl.MAX_CHARS_PER_KEY_WORD directories
	 * per key word. database files use a .database suffix
	 * eg shard with key "IM~CUTE", MAX_CHARS_PER_KEY_WORD=2 has path:
	 * \<parent database id>\<database dir string>\I\M\~\C\U\IM~CUTE.database
	 * (where <database dir string> is MarkovDatabaseImpl.DATABASE_DIRECTORY_NAME)
	 */
	protected Path path;
	/*
	 * holds the actual data for this shard
	 * database maps bigram->followingwordset representing the words following that bigram
	 * starts as a light arraylist-based implementation and switches to a hashmap-based one
	 * once the followingwordset reaches a certain size
	 * goal is to minimize memory use as much as possible, sacrificing speed if necessary (to a point...)
	 */
	protected DatabaseWrapper database;
	
	DatabaseShard(String parentId, String key, String parentPath) {
		this.parentDatabaseId = parentId;
		this.key = key;
		String pathString = this.determinePath(parentPath);
		this.path = Paths.get(pathString);
		this.database = new DatabaseWrapper(this.key, this.parentDatabaseId);
	}
	
	/*
	 * used to add a single occurrence of the given followingWord for the given bigram
	 * returns true if new entry in followingwordset was created as a result of this call
	 * 
	 * note this has concurrency problems (resulting from followingwordset operations? + 
	 * replacing the followingwordset in some cases) which will rarely result in calling this
	 * method effectively doing nothing; this isn't a major issue but yeah
	 * can be avoided by ensuring that this method is called in a synchronized environment?
	 * ie, use in atomic compute() from in ShardCache
	 */
	boolean addFollowingWord(Bigram bigram, String followingWord) {
		FollowingWordSet followingWordSet = this.database.get(bigram);
		if(followingWordSet != null) {
			if(followingWordSet instanceof TinyFollowingWordSet) {
				if(followingWordSet.size() >= SMALL_WORD_SET_THRESHOLD) {
					this.database.put(bigram, new SmallFollowingWordSet(followingWordSet, followingWord, bigram, this.parentDatabaseId));
				} else {
					this.database.put(bigram, TinyFollowingWordSet.of(followingWordSet, followingWord));
				}
			} else {
				followingWordSet.addWord(followingWord);
				//check for replacing small set with large
				//better way to do this?
				if(followingWordSet.size() >= LARGE_WORD_SET_THRESHOLD && followingWordSet instanceof SmallFollowingWordSet) {
					this.database.put(bigram, new LargeFollowingWordSet((SmallFollowingWordSet)followingWordSet));
				}
			}
			return false;
		} else {
			return this.addNewBigram(bigram, followingWord);
		}
	}
	
	/*
	 * start with tinyfollowingwordset
	 * returns true if a new entry was created in the database as a result of this call
	 * (effectively always unless some unexpected concurrent stuff has happened)
	 */
	private boolean addNewBigram(Bigram bigram, String followingWord) {
		return this.database.putIfAbsent(bigram, TinyFollowingWordSet.of(followingWord)) == null;
	}
	
	/*
	 * gets a weighted random word that follows the given bigram according to the shard
	 * throws IllegalArgumentException if the given bigram isn't present in the shard
	 * (shouldn't happen normally, but could if there are issues when adding words)
	 */
	String getFollowingWord(Bigram bigram) throws IllegalArgumentException {
		FollowingWordSet followingWordSet = this.database.get(bigram);
		if(followingWordSet == null) throw new IllegalArgumentException(bigram.toString() + " not found in " + this.toString());
		
		return followingWordSet.getRandomWeightedWord();
	}
	
	/*
	 * checks for existence of the given followingword for the given bigram
	 * returns true if the bigram exists in the database and the given followingword
	 * has been recorded for that bigram at least once, and false otherwise (ie the given
	 * followingword has never been used for the given bigram, or the given bigram has 
	 * never been used)
	 */
	boolean contains(Bigram bigram, String followingWord) {
		FollowingWordSet followingWordSet = this.database.get(bigram);
		if(followingWordSet == null) return false;
		
		return followingWordSet.contains(followingWord);
	}
	
	/*
	 * same as contains(Bigram,String), but checks if the given string has been used
	 * for the given bigram at least the given number of times
	 * contains(Bigram,String) is equivalent to contains(Bigram,String,1)
	 */
	boolean contains(Bigram bigram, String followingWord, int count) {
		FollowingWordSet followingWordSet = this.database.get(bigram);
		if(followingWordSet == null) return false;
		
		return followingWordSet.contains(followingWord, count);
	}
	
	/*
	 * TODO if remove below a set size threshold, replace
	 * eg if remove when at 24 items, switch from largefws to smallfws
	 * 
	 * removes a single occurrence of the given followingWord for the given bigram
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
				+ followingWord + "' from fws for bigram " + bigram + " in " + this + ": no fws not found for given bigram");
		
		if(followingWordSet instanceof TinyFollowingWordSet) {
			this.database.put(bigram, TinyFollowingWordSet.remove((TinyFollowingWordSet)followingWordSet, followingWord));
			FollowingWordSet newSet = this.database.get(bigram);
			if(followingWordSet.size() == newSet.size()) {
				throw new FollowingWordRemovalException("illegal attempt to remove word '" + followingWord
						+ "' from tiny fws for bigram " + bigram + " in " + this + "! old fws: " + followingWordSet 
						+ ", new fws: " + this.database.get(bigram));	
			}
			if(newSet.isEmpty()) {
				this.remove(bigram);
			}
		} else if(followingWordSet.remove(followingWord)) {
			//small or large followingwordset. remove word and change implementation if necessary

			if(followingWordSet instanceof SmallFollowingWordSet && followingWordSet.size() < SMALL_WORD_SET_THRESHOLD) {
				this.database.put(bigram, TinyFollowingWordSet.of(followingWordSet));
			}
			else if(followingWordSet instanceof LargeFollowingWordSet && followingWordSet.size() < LARGE_WORD_SET_THRESHOLD) {
				this.database.put(bigram, new SmallFollowingWordSet(followingWordSet));
			}
			//set was small or large, so set size was at least 4, so set can't be empty
			//& therefore no need to possibly remove entry for bigram from database
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
	 * serializing is done via fast-serialization library. json currently not supported
	 */
	void save(SaveType saveType) {
		if(saveType == SaveType.JSON) {
			try {
				this.saveAsText();
			} catch (IOException e) {
				logger.warn(this + ": couldn't save (json)! ex: " + e.getLocalizedMessage(), e);
			}
		} else {
			try {
				this.saveAsObject();
			} catch (IOException e) {
				logger.warn(this + ": couldn't save (serialize)! ex: " + e.getLocalizedMessage(), e);
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
				logger.error(this + ": fatal exception when trying to load (json)! ex: " + e.getLocalizedMessage(), e);
				throw new RuntimeException(e);
			}
		} else {
			try {
				this.loadFromObject();
			} catch (FileNotFoundException e) {
//				logger.info("couldn't load (deserialize) " + this.toString() + ", file not found (first load?) ex: " + e.getLocalizedMessage());
			} catch (IOException e) {
				logger.error(this + ": fatal exception when trying to load (deserialize)! ex: " + e.getLocalizedMessage(), e);
				throw new UncheckedIOException(e);
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
		FileOutputStream fileOutputStream = null;
		try {
			fileOutputStream = new FileOutputStream(this.path.toString());
		} catch (FileNotFoundException ex) {
			//probably first load and parent directory doesn't exist. create it and try again
			this.path.toFile().getParentFile().mkdirs();
			fileOutputStream = new FileOutputStream(this.path.toString());
		}
		FSTObjectOutput out = CONF.getObjectOutput(fileOutputStream);
		out.writeObject(this.database, DatabaseWrapper.class);
		out.flush();
		fileOutputStream.close();
	}

	void loadFromObject() throws IOException {
		try (FileInputStream fileInputStream = new FileInputStream(this.path.toString())) {
			FSTObjectInput in = CONF.getObjectInput(fileInputStream);
			try {
				this.database = (DatabaseWrapper) in.readObject(DatabaseWrapper.class);
			} catch (Exception e) {
				//have to do this, because FSTObjectInput.readObject(Class) throws Exception...
				throw new IOException(e);
			}
		} catch (FileNotFoundException ex) {
			//nothing to load, probably first run. do nothing
		}
	}
	
	/*
	 * obtain the path for the file representing this shard on the local disk
	 * paths are determined by the shard's key, and are separated into a new 
	 * directory for each character in the key up to MarkovDatabaseImpl.DIRECTORIES_PER_KEY_WORD
	 * eg shard with key "IM~CUTE", DIRECTORIES_PER_KEY_WORD=2 has path:
	 * \<parent database id>\<database dir string>\I\M\~\C\U\IM~CUTE.database
	 * (where <database dir string> is MarkovDatabaseImpl.DATABASE_DIRECTORY_NAME)
	 * note that all possible characters in a key are regular uppercase ascii english
	 * alphabet characters (A-Z) to represent that letter, 0 to represent regular ascii
	 * numbers (0-9), ! to represent punctuation (as determined by matching against 
	 * regex \\p{Punct}), and @ to represent all other characters
	 * 
	 * returns the string representing the path for this shard
	 * param parentPath should be the part of the path that isn't based on key
	 * (ie, the \<parent database id>\<database dir string> part)
	 */
	private String determinePath(String parentPath) {
		StringBuilder sb = new StringBuilder(parentPath);
		sb.append(File.separator);
		if(this.key != MarkovDatabaseImpl.START_KEY) {
			int index = 0;
			String words[] = this.key.split("~");
			String word = words[0];
			while(index < word.length() && index < MarkovDatabaseImpl.DIRECTORIES_PER_KEY_WORD) {
				sb.append(word.charAt(index));
				sb.append(File.separator);
				index++;
			}
			sb.append("~");
			sb.append(File.separator);
			index = 0;
			word = words[1].split("\\.")[0];
			while(index < word.length() && index < MarkovDatabaseImpl.DIRECTORIES_PER_KEY_WORD) {
				sb.append(word.charAt(index));
				sb.append(File.separator);
				index++;
			}
		}
		sb.append(this.key);
		sb.append(".database");
		return sb.toString();
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("DatabaseShard [parentDatabaseId=");
		builder.append(parentDatabaseId);
		builder.append(", key=");
		builder.append(key);
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
	
	void writeDatabaseStringToOutput(BufferedWriter output) throws IOException {
		//sort collection of entries before printing, sorting on the bigram (word1 then word2)
		List<Map.Entry<Bigram, FollowingWordSet>> entries = new ArrayList<>(this.database.entrySet()); 
		Collections.sort(entries, Comparator.comparing(entry -> entry.getKey()));
		for(Map.Entry<Bigram, FollowingWordSet> bigramEntry : entries) {
			StringBuilder sb = new StringBuilder();
			sb.append("(");
			sb.append(bigramEntry.getKey().getWord1());
			sb.append(", ");
			sb.append(bigramEntry.getKey().getWord2());
			sb.append(") -> {");
			sb.append("count=");
			sb.append(bigramEntry.getValue().size());
			sb.append(", ");
			sb.append(bigramEntry.getValue().toStringPlain());
			sb.append("}");
			output.append(sb.toString());
			output.newLine();
		}
	}
}
