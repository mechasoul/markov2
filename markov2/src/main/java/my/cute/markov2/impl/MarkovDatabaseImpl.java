package my.cute.markov2.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
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

public class MarkovDatabaseImpl implements MarkovDatabase {
	
	private static final Logger logger = LoggerFactory.getLogger(MarkovDatabaseImpl.class);
	
	private static final Pattern PUNCTUATION = Pattern.compile("\\p{Punct}");
	static final String START_TOKEN = MyStringPool.INSTANCE.intern("<_start>");
	static final String TOTAL_TOKEN = MyStringPool.INSTANCE.intern("<_total>");
	static final String END_TOKEN = MyStringPool.INSTANCE.intern("<_end>");
	private static final Map<String, String> tokenReplacements;
	static final String ZERO_DEPTH_PREFIX = "~full_database";
	static final String START_PREFIX = "~start";
	static final int MAX_WORDS_PER_LINE = 256;
	private static final String DATABASE_DIRECTORY_NAME = "~database";
	private static final String BACKUP_DIRECTORY_NAME = "~backups";
	
	static {
		tokenReplacements = new HashMap<String, String>(4, 1f);
		tokenReplacements.put(START_TOKEN, MyStringPool.INSTANCE.intern("start"));
		tokenReplacements.put(TOTAL_TOKEN, MyStringPool.INSTANCE.intern("total"));
		tokenReplacements.put(END_TOKEN, MyStringPool.INSTANCE.intern("end"));
	}
	
	private final String id;
	private int depth;
	private final String path;
	private final ShardCache shardCache;
	
	MarkovDatabaseImpl(MarkovDatabaseBuilder builder) {
		this.id = builder.getId();
		this.path = builder.getParentPath() + File.separator + this.id;
		this.depth = builder.getDepth();
		this.shardCache = new ShardCache(this.id, builder.getShardCacheSize(), builder.getDepth(), this.path 
				+ File.separator + DATABASE_DIRECTORY_NAME, builder.getSaveType(),
				builder.getExecutorService(), builder.getFixedCleanupThreshold());
		new File(this.path + File.separator + BACKUP_DIRECTORY_NAME).mkdirs();
	}
	
	@Override
	public void processLine(List<String> words) {
		if(words.size() == 0) {
			logger.warn("attempt to process empty word array in " + this.toString());
			return;
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
	}
	
	private void addFollowingWordForBigram(Bigram bigram, String followingWord) {
		this.shardCache.addFollowingWord(this.getPrefix(bigram.getWord1()), bigram, followingWord);
	}
	
	private DatabaseShard getShard(Bigram bigram) {
		String prefix = this.getPrefix(bigram.getWord1());
		return this.shardCache.get(prefix);
	}
	
	
	/*
	 * accepts a nonempty nonwhitespace word and returns the appropriate prefix for
	 * the database
	 * prefix will vary depending on db depth (eg apple has prefix A in depth 1, AP
	 * in depth 2)
	 * prefix chars are ascii letters, 0 for numbers, ! for punctuation, @ for other
	 * standard prefixes always have same length as depth, filling in missing characters
	 * with A (eg "i" in depth 2 has prefix IA
	 */
	private String getPrefix(String word) {
		//special cases for start token and 0 depth database
		if(word.equals(START_TOKEN)) return START_PREFIX;
		if(this.depth == 0) return ZERO_DEPTH_PREFIX;
		
		StringBuilder prefix = new StringBuilder();
		int index = 0;
		
		//loop should happen at least once; depth > 0 by above check and word should be nonempty
		while(index < this.depth && index < word.length()) {
			char ch = word.charAt(index);
			if (ch >= '0' && ch <= '9') {
				prefix.append("0");
			}
			//strictly use ascii letters
			else if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')) {
				prefix.append(Character.toUpperCase(ch));
			}
			else if (PUNCTUATION.matcher(String.valueOf(ch)).matches()) {
				prefix.append("!");
			} else {
				prefix.append("@");
			}
			index++;
		}
		//fill in remainder up to depth
		while(index < depth) {
			prefix.append("A");
			index++;
		}
		
		//StringBuilder should be nonempty since above loop happened at least once
		return MyStringPool.INSTANCE.intern(prefix.toString());
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
		this.shardCache.cleanUp();
		StringBuilder sb = new StringBuilder();
		sb.append(startingWord);
		int wordCount = 1;
		Bigram currentBigram = new Bigram(START_TOKEN, startingWord);
		String nextWord = this.getRandomWeightedNextWord(currentBigram);
		while(!nextWord.equals(END_TOKEN) && wordCount < MAX_WORDS_PER_LINE) {
			sb.append(" ");
			sb.append(nextWord);
			wordCount++;
			
			currentBigram = new Bigram(currentBigram.getWord2(), nextWord);
			nextWord = this.getRandomWeightedNextWord(currentBigram);
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
				+ ", constructing default FollowingWordSet w/ END_TOKEN");
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
			//they do for add/remove but those actually modify the db and contains doesnt so maybe this is ok?
			//seems fine from testing but something to consider
//			return this.contains(pair.getLeft(), pair.getRight(), count);
		});
	}
	
	private boolean contains(Bigram bigram, String followingWord, int count) {
		return this.shardCache.contains(this.getPrefix(bigram.getWord1()), bigram, followingWord, count);
	}
	
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
		this.shardCache.removeFollowingWord(this.getPrefix(bigram.getWord1()), bigram, followingWord);
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
		this.save();
		
		final Path databaseDirectory = Paths.get(this.path + File.separator + DATABASE_DIRECTORY_NAME);
		final Path targetFile = this.getBackupPath(backupName);
		Path createdFile;
		try {
			createdFile = Files.createFile(targetFile);
		} catch (FileAlreadyExistsException ex) {
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
				throw ex;
			}
		}
		
		logger.info(this + "-save-" + backupName + ": finished saving backup");
		return targetFile;
	}
	
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
					throw ex;
				}
				logger.info(this + "-load-" + backupName + ": finished unpacking backup. deleting temp backup");
			}
		}
		Files.delete(tempBackup);
		logger.info(this + "-load-" + backupName + ": temp backup deleted. backup successfully loaded");
	}
	
	/*
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
	
	private Path getBackupPath(String backupName) {
		return Paths.get(this.path + File.separator + BACKUP_DIRECTORY_NAME
				+ File.separator + this.id + "_" + backupName + ".zip");
	}
	
	private ReentrantLock getSaveLock() {
		return this.shardCache.getSaveLock();
	}
	
	private ReentrantLock getLoadLock() {
		return this.shardCache.getLoadLock();
	}

	@Override
	public void exportToTextFile() {
		/*
		 * builds human readable version of database
		 * checks all .database files in directory
		 * like everything else, breaks if outside sources modify db files
		 */
		String timeStamp = new SimpleDateFormat("yyyyMMdd-HHmmss").format(Calendar.getInstance().getTime());
		String path = this.path + File.separator + this.id + "_" + timeStamp + ".txt";
		for(File file : FileUtils.listFiles(new File(this.path), FileFilterUtils.suffixFileFilter(".database"), TrueFileFilter.TRUE)) {
			try {
				this.shardCache.writeDatabaseShardString(file, path);
			} catch (IOException e) {
				logger.error("exception in trying to export " + this.toString() + " to text file, aborting! file: ' "
						+ file.toString() + "', exception: " + e.getLocalizedMessage());
				e.printStackTrace();
				break;
			}
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
