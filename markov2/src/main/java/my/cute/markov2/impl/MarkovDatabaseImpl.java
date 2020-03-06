package my.cute.markov2.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import my.cute.markov2.MarkovDatabase;
import my.cute.markov2.exceptions.FollowingWordRemovalException;

/*
 * implementation of MarkovDatabase
 * prioritizes memory over speed
 * 
 * TODO I don't really like the general way i've decided to deal with concurrency problems
 * i think i'm over-synchronizing because i don't fully understand how caffeine cache works
 * i'd like to eliminate the addFollowingWord(), removeFollowingWord(), etc methods from
 * ShardCache and just call them directly on the shard via like
 * this.shardCache.get(bigram).addFollowingWord(...)
 * but need to figure out where the problems are stemming from in order to do that
 * (problems arent even a big deal it's just a very rare instance of a word not being
 * added when it should, etc. but it's a big enough deal to bug me)
 * also see DatabaseShard.addFollowingWord(Bigram, String)
 */
public class MarkovDatabaseImpl implements MarkovDatabase {
	
	private static final Logger logger = LoggerFactory.getLogger(MarkovDatabaseImpl.class);
	
	private static final Pattern PUNCTUATION = Pattern.compile("\\p{Punct}");
	/*
	 * special tokens used for start/end of line indicators
	 * chosen to be strings unlikely to be used by a person
	 * are stripped from lines during processing
	 */
	static final String START_TOKEN = MyStringPool.INSTANCE.intern("<_start>");
	static final String END_TOKEN = MyStringPool.INSTANCE.intern("<_end>");
	private static final Map<String, String> tokenReplacements;
	static final String START_KEY = "~start";
	static final int MAX_WORDS_PER_LINE = 256;
	static final int DIRECTORIES_PER_KEY_WORD = 1;
	static final int MAX_CHARS_PER_KEY_WORD = 1;
	private static final String DATABASE_DIRECTORY_NAME = "~database";
	private static final String BACKUP_DIRECTORY_NAME = "~backups";
	
	static {
		tokenReplacements = new HashMap<String, String>(3, 1f);
		tokenReplacements.put(START_TOKEN, MyStringPool.INSTANCE.intern("start"));
		tokenReplacements.put(END_TOKEN, MyStringPool.INSTANCE.intern("end"));
	}
	
	private final String id;
	private final String path;
	private final ShardCache shardCache;
	
	MarkovDatabaseImpl(MarkovDatabaseBuilder builder) {
		this.id = builder.getId();
		this.path = builder.getParentPath() + File.separator + this.id;
		this.shardCache = new ShardCache(this.id, builder.getShardCacheSize(), this.path 
				+ File.separator + DATABASE_DIRECTORY_NAME, SaveType.SERIALIZE,
				builder.getExecutorService(), builder.getFixedCleanupThreshold());
		//ensure necessary directories exist during db creation
		new File(this.path + File.separator + BACKUP_DIRECTORY_NAME).mkdirs();
	}
	
	@Override
	public boolean processLine(List<String> words) {
		if(words.size() == 0) {
			return false;
		}
		
		//at least one element is present by above
		Bigram currentBigram = new Bigram(START_TOKEN, stripTokens(MyStringPool.INSTANCE.intern(words.get(0))));
		String nextWord = "";
		int wordIndex = 1;
		while(wordIndex < words.size()) {
			nextWord = stripTokens(MyStringPool.INSTANCE.intern(words.get(wordIndex)));
			this.addFollowingWordForBigram(currentBigram, nextWord);
			currentBigram = new Bigram(currentBigram.getWord2(), nextWord);
			wordIndex++;
		}
		this.addFollowingWordForBigram(currentBigram, END_TOKEN);
		return true;
	}
	
	private void addFollowingWordForBigram(Bigram bigram, String followingWord) {
		this.shardCache.addFollowingWord(this.getKey(bigram), bigram, followingWord);
	}
	
	private DatabaseShard getShard(Bigram bigram) {
		String key = this.getKey(bigram);
		return this.shardCache.get(key);
	}
	
	
	/*
	 * accepts a bigram and returns the appropriate key for the database
	 * keys are special strings that represent the bigrams used for that part of the database
	 * keys are determined by both words of the bigram and MAX_CHARS_PER_KEY_WORD
	 * each word has up to MAX_CHARS_PER_KEY_WORD chars representing it in the key, so max key
	 * length is 2 * MAX_CHARS_PER_KEY_WORD + 1 (if a word has less chars than MAX_CHARS_PER_KEY_WORD
	 * then that part of the key will just have as many chars are in that word)
	 * key chars are ascii letters, 0 for numbers, ! for punctuation, @ for other, and ~
	 * used to represent the space between word1 and word2
	 * eg bigram (im, gay), MAX_CHARS_PER_KEY_WORD=3 has key "IM~GAY"
	 * bigram (999, .things), MAX_CHARS_PER_KEY_WORD=2 has key "00~!T"
	 * bigram (abcdefghij, hellohowareyoutoday), MAX_CHARS_PER_KEY_WORD=10 has key "ABCDEFGHIJ~HELLOHOWAR"
	 */
	private String getKey(Bigram bigram) {
		//special case for start token
		if(bigram.getWord1().equals(START_TOKEN)) return START_KEY;
		
		StringBuilder key = new StringBuilder();
		int index = 0;
		
		//loop should happen at least once; word should be nonempty
		String word = bigram.getWord1();
		while(index < MAX_CHARS_PER_KEY_WORD && index < word.length()) {
			char ch = word.charAt(index);
			if (ch >= '0' && ch <= '9') {
				key.append("0");
			}
			//strictly use ascii letters
			else if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')) {
				key.append(Character.toUpperCase(ch));
			}
			else if (PUNCTUATION.matcher(String.valueOf(ch)).matches()) {
				key.append("!");
			} else {
				key.append("@");
			}
			index++;
		}
		key.append("~");
		//now do second word
		index = 0;
		word = bigram.getWord2();
		while(index < MAX_CHARS_PER_KEY_WORD && index < word.length()) {
			char ch = word.charAt(index);
			if (ch >= '0' && ch <= '9') {
				key.append("0");
			}
			//strictly use ascii letters
			else if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')) {
				key.append(Character.toUpperCase(ch));
			}
			else if (PUNCTUATION.matcher(String.valueOf(ch)).matches()) {
				key.append("!");
			} else {
				key.append("@");
			}
			index++;
		}
		
		//StringBuilder should be nonempty since above loops happened at least once
		return MyStringPool.INSTANCE.intern(key.toString());
	}
	
	private StartDatabaseShard getStartShard() {
		return this.shardCache.getStartShard();
	}
	
	@Override
	public String generateLine() {
		try {
			return this.generateLine(this.getStartShard().getRandomWeightedStartWord());
		} catch (IllegalArgumentException ex) {
			logger.warn("illegal argument exception thrown in line generation (empty database?) in db " + this);
			return "??";
		}
	}
	
	@Override
	public String generateLine(String startingWord) {
		StringBuilder sb = new StringBuilder();
		sb.append(startingWord);
		int wordCount = 1;
		Bigram currentBigram = new Bigram(START_TOKEN, startingWord);
		synchronized(this.getLoadLock()) {
			String nextWord = this.getRandomWeightedNextWord(currentBigram);
			while(!nextWord.equals(END_TOKEN) && wordCount < MAX_WORDS_PER_LINE) {
				sb.append(" ");
				sb.append(nextWord);
				wordCount++;
				
				currentBigram = new Bigram(currentBigram.getWord2(), nextWord);
				nextWord = this.getRandomWeightedNextWord(currentBigram);
			}
		}
		
		return sb.toString();
	}
	
	private String getRandomWeightedNextWord(Bigram bigram) {
		try {
			return this.getShard(bigram).getFollowingWord(bigram);
		} catch (IllegalArgumentException ex) {
			/*
			 * thrown when no followingwordset is found for the given bigram
			 * any given bigram that can be constructed from db should have a following word
			 * so this indicates something has gone wrong (eg some words not processed from a line)
			 * add a general following word set by having the given bigram end a message
			 */
			logger.warn(this + " couldn't find following word for " + bigram + " (ex: " + ex.getLocalizedMessage()
				+ "), constructing default FollowingWordSet w/ END_TOKEN");
			this.addFollowingWordForBigram(bigram, END_TOKEN);
			return END_TOKEN;
		}
	}
	
	@Override
	public boolean contains(List<String> words) {
		if(words.size() == 0) {
			logger.warn("attempt to remove empty word list in " + this.toString());
			return true;
		}
		
		//this map will track the number of occurrences of each bigram->word pair in the given list
		//necessary to be able to check that the database contains the given number of occurrences of words
		TObjectIntMap<Pair<Bigram, String>> bigramWordCounts = new TObjectIntHashMap<>(words.size() * 4 / 3);
		//at least one element present by above check so this get() never throws exception
		Bigram currentBigram = new Bigram(START_TOKEN, stripTokens(MyStringPool.INSTANCE.intern(words.get(0))));
		for(int i=1; i < words.size(); i++) {
			String followingWord = stripTokens(MyStringPool.INSTANCE.intern(words.get(i)));
			bigramWordCounts.adjustOrPutValue(new ImmutablePair<Bigram, String>(currentBigram, followingWord), 1, 1);
			currentBigram = new Bigram(currentBigram.getWord2(), followingWord);
		}
		bigramWordCounts.adjustOrPutValue(new ImmutablePair<Bigram, String>(currentBigram, END_TOKEN), 1, 1);
		
		//now check each entry in the map to make sure it's contained in db
		//if any contains() returns false, forEachEntry() call terminates and returns false
		//otherwise, returns true once finished
		return bigramWordCounts.forEachEntry((pair, count) ->
		{
			return this.getShard(pair.getLeft()).contains(pair.getLeft(), pair.getRight(), count);
			//TODO do any concurrency problems occur from calling contains directly on the shard as above?
			//they do for add/remove but those actually modify the db and contains doesnt
			//seems fine from testing but something to consider
//			return this.contains(pair.getLeft(), pair.getRight(), count);
		});
	}
	
//	alternate contains that defers to cache so we can do it atomically. seems unnecessary as above. unused
//	private boolean contains(Bigram bigram, String followingWord, int count) {
//		return this.shardCache.contains(this.getKey(bigram), bigram, followingWord, count);
//	}
	
	@Override
	public boolean removeLine(List<String> words) throws FollowingWordRemovalException {
		
		if(words.size() == 0) {
			logger.warn("attempt to remove empty word list in " + this.toString());
			return false;
		}
		
		//preemptively check for presence of entire line in db to avoid partial removal
		if(!this.contains(words)) {
			return false;
		}
		
		//now perform the actual removals
		Bigram currentBigram = new Bigram(START_TOKEN, stripTokens(MyStringPool.INSTANCE.intern(words.get(0))));
		for(int i=1; i < words.size(); i++) {
			String followingWord = stripTokens(MyStringPool.INSTANCE.intern(words.get(i)));
			this.removeFollowingWordForBigram(currentBigram, followingWord);
			currentBigram = new Bigram(currentBigram.getWord2(), followingWord);
		}
		this.removeFollowingWordForBigram(currentBigram, END_TOKEN);
		return true;
	}
	
	private void removeFollowingWordForBigram(Bigram bigram, String followingWord) throws FollowingWordRemovalException {
		this.shardCache.removeFollowingWord(this.getKey(bigram), bigram, followingWord);
	}

	@Override
	public void save() {
		this.shardCache.save();
	}
	
	@Override
	public void load() {
		this.shardCache.load();
	}
	
	@Override
	public Path saveBackup(String backupName) throws IOException {
		logger.info(this + "-save-" + backupName + ": beginning saving backup");
		this.save();
		
		final Path databaseDirectory = Paths.get(this.path + File.separator + DATABASE_DIRECTORY_NAME);
		final Path targetFile = this.getBackupPath(backupName);
		Path createdFile;
		try {
			createdFile = Files.createFile(targetFile);
		} catch (FileAlreadyExistsException ex) {
			//overwriting a backup. back it up and delete it when finished, so it can be restored if unsuccessful
			createdFile = Files.createTempFile(Paths.get(this.path + File.separator + BACKUP_DIRECTORY_NAME), null, null);
			Files.copy(targetFile, Paths.get(targetFile.toString() + ".bak"), StandardCopyOption.REPLACE_EXISTING);
		}
		synchronized(this.getSaveLock()) {
			try {
				this.pack(databaseDirectory, createdFile);
				//zip created, need to move files around now if backup already existed
				if(!createdFile.equals(targetFile)) {
				
					Files.delete(targetFile);
					Files.copy(createdFile, targetFile);
					Files.delete(createdFile);
					Files.delete(Paths.get(targetFile.toString() + ".bak"));
				} 
			} catch (Exception ex) {
				//encountered exception. attempt to recover the backup from our backup of the backup
				//if it exists
				if(!createdFile.equals(targetFile)) {
					logger.error("exception encountered during backup creation for backup "
							+ backupName + " in " + this + ": " + ex.toString());
					logger.error("attempting to restore previously existing backup");
					Files.deleteIfExists(targetFile);
					Files.copy(Paths.get(targetFile.toString() + ".bak"), targetFile, StandardCopyOption.REPLACE_EXISTING);
					Files.delete(Paths.get(targetFile.toString() + ".bak"));
					logger.error("successfully recovered backup " + backupName + " in " + this + ". backup creation aborted, "
							+ "rethrowing exception");
				}
				throw new IOException(ex);
			}
		}
		
		logger.info(this + "-save-" + backupName + ": finished saving backup");
		return targetFile;
	}
	
	/*
	 * takes a given directory and packs its contents (not itself) as a zip file into the given file
	 */
	private void pack(Path directoryToPack, Path zipFile) throws IOException {
		try (OutputStream os = Files.newOutputStream(zipFile);
				ZipOutputStream zipStream = new ZipOutputStream(os);
		) {
			try (Stream<Path> stream = Files.walk(directoryToPack))
			{
				stream.filter(path -> !Files.isDirectory(path))
				.forEach(path ->
				{
					ZipEntry zipEntry = new ZipEntry(directoryToPack.relativize(path).toString());
					try {
						zipStream.putNextEntry(zipEntry);
						Files.copy(path, zipStream);
						zipStream.flush();
						zipStream.closeEntry();
					} catch (IOException ex) {
						throw new UncheckedIOException(ex);
					}
				});
			}
		} catch (UncheckedIOException ex) {
			throw ex.getCause();
		}
	}

	@Override
	public void loadBackup(String backupName) throws FileNotFoundException, IOException {
		final Path targetBackup = this.getBackupPath(backupName);
		
		if(!Files.isRegularFile(targetBackup)) {
			throw new FileNotFoundException("backup '" + backupName + "' not found for " + this);
		}
		logger.info("beginning loading backup of database " + this + " (from backup '" + backupName + "')");
		//first save a backup of current database state so we can try to restore it if load fails
		this.shardCache.saveAndClear();
		String tempBackupName = new SimpleDateFormat("yyyyMMdd-HHmmss").format(Calendar.getInstance().getTime()) + "_tmp";
		logger.info(this + "-load-" + backupName + ": saving temp backup '" + tempBackupName + "' for recovery");
		Path tempBackup = this.saveBackup(tempBackupName);
		logger.info(this + "-load-" + backupName + ": finished saving temp backup. deleting current database files");
		
		synchronized(this.getLoadLock()) {
			synchronized(this.getSaveLock()) {
				//delete all current database files
				final String databaseDirectory = this.path + File.separator + DATABASE_DIRECTORY_NAME;
				FileUtils.deleteDirectory(new File(databaseDirectory));
				
				logger.info(this + "-load-" + backupName + ": finished deleting database. unpacking backup");
				try {
					this.unpack(targetBackup, Paths.get(databaseDirectory));
				} catch (Exception ex) {
					logger.error(this + "-load-" + backupName + ": encountered exception when trying to unpack backup: "
							+ ex.getMessage());
					ex.printStackTrace();
					logger.error(this + "-load-" + backupName + ": attempting to load from temp backup");
					this.unpack(tempBackup, Paths.get(databaseDirectory));
					logger.error(this + "-load-" + backupName + ": successfully unpacked temp backup. deleting temp backup");
					Files.delete(tempBackup);
					logger.error(this + "-load-" + backupName + ": temp backup deleted. aborting backup load");
					throw new IOException(ex);
				}
				logger.info(this + "-load-" + backupName + ": finished unpacking backup. deleting temp backup");
			}
			this.load();
		}
		Files.delete(tempBackup);
		logger.info(this + "-load-" + backupName + ": temp backup deleted. backup successfully loaded");
	}
	
	/*
	 * unpacks the given file as a zip file into the given directory
	 * probably needs some kind of validation check for zipslip vulnerability
	 * but skipping that for now
	 */
	private void unpack(Path zipFile, Path directory) throws IOException {
		try (InputStream is = Files.newInputStream(zipFile);
				ZipInputStream zipStream = new ZipInputStream(is);
		) {
			ZipEntry zipEntry;
			while((zipEntry = zipStream.getNextEntry()) != null) {
				final Path outputFile = directory.resolve(zipEntry.getName());
				if(zipEntry.isDirectory()) {
					try {
						Files.createDirectory(outputFile);
					} catch (FileAlreadyExistsException ex) {
						//continue
					}
				} else {
					Files.createDirectories(outputFile.getParent());
					Files.copy(zipStream, outputFile);
				}
			}
		}	
	}
	
	@Override 
	public boolean deleteBackup(String backupName) throws IOException {
		try {
			Files.delete(this.getBackupPath(backupName));
		} catch (NoSuchFileException ex) {
			return false;
		}
		logger.info(this + "-delete-" + backupName + ": successfully deleted backup");
		return true;
	}
	
	/*
	 * given a backup name, returns the path to the file that will be used for that backup
	 */
	private Path getBackupPath(String backupName) {
		return Paths.get(this.path + File.separator + BACKUP_DIRECTORY_NAME
				+ File.separator + this.id + "_" + backupName + ".zip");
	}
	
	@Override
	public void clear() throws IOException {
		logger.info(this + ": clearing database");
		this.shardCache.saveAndClear();
		this.shardCache.getStartShard().clear();
		FileUtils.deleteDirectory(new File(this.path + File.separator + DATABASE_DIRECTORY_NAME));
		this.load();
		logger.info(this + ": finished clearing database");
	}
	
	private Object getSaveLock() {
		return this.shardCache.getSaveLock();
	}
	
	private Object getLoadLock() {
		return this.shardCache.getLoadLock();
	}

	/*
	 * builds human readable version of database
	 * checks all .database files in directory
	 * like everything else, breaks if outside sources modify db files
	 */
	@Override
	public void exportToTextFile() {
		this.save();
		String timeStamp = new SimpleDateFormat("yyyyMMdd-HHmmss").format(Calendar.getInstance().getTime());
		String path = this.path + File.separator + this.id + "_" + timeStamp + ".txt";
		try (BufferedWriter output = Files.newBufferedWriter(Paths.get(path), StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
			List<File> files = new ArrayList<>(FileUtils.listFiles(new File(this.path), FileFilterUtils.suffixFileFilter(".database"), TrueFileFilter.TRUE));
			Collections.sort(files, (first, second) ->
			{
				return first.toString().compareTo(second.toString());
			});
			for(File file : files) {
				try {
					this.shardCache.writeDatabaseShardString(output, file);
				} catch (IOException e) {
					logger.error("exception in trying to export " + this.toString() + " to text file, aborting! file: ' "
							+ file.toString() + "', exception: " + e.getLocalizedMessage());
					e.printStackTrace();
					break;
				}
			}
		} catch (IOException e1) {
			logger.error("general io exception in trying to export " + this.toString() + " to text file, aborting! exception: "
					+ e1.getLocalizedMessage());
			e1.printStackTrace();
		}
	}

	@Override
	public String getId() {
		return this.id;
	}
	
	/*
	 * checks an input string to make sure it's not a reserved token
	 * returns the string if it isn't a token, or a replacement word if it is
	 */
	private static String stripTokens(String input) {
		String replacedWord = tokenReplacements.get(input);
		if(replacedWord == null) {
			return input;
		} else {
			return replacedWord;
		}
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("MarkovDatabaseImpl-");
		sb.append(this.id);
		return sb.toString();
	}

	
	
}
