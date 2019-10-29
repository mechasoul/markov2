package my.cute.markov2.impl;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

import my.cute.markov2.MarkovDatabase;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MarkovDatabaseImpl implements MarkovDatabase {
	
	private class AddFollowingWordTask implements Runnable {
		
		private final Bigram bigram;
		private final String followingWord;
		
		private AddFollowingWordTask(Bigram b, String f) {
			this.bigram = b;
			this.followingWord = f;
		}

		@Override
		public void run() {
			addFollowingWordForBigram(this.bigram, this.followingWord);
		}
		
	}

	private static final Logger logger = LoggerFactory.getLogger(MarkovDatabaseImpl.class);
	
	private static final Pattern PUNCTUATION = Pattern.compile("\\p{Punct}");
	static final String START_TOKEN = "<_start>";
	static final String TOTAL_TOKEN = "<_total>";
	static final String END_TOKEN = "<_end>";
	private static final Map<String, String> tokenReplacements;
	public static final String ZERO_DEPTH_PREFIX = "~database";
	static final String START_PREFIX = "~start";
	static final int MAX_WORDS_PER_LINE = 256;
	
	static {
		tokenReplacements = new HashMap<String, String>(4);
		tokenReplacements.put(START_TOKEN, "start");
		tokenReplacements.put(TOTAL_TOKEN, "total");
		tokenReplacements.put(END_TOKEN, "end");
	}
	
	private final String id;
	private int depth;
	private final String path;
	private final ShardCache shardCache;
	private final ShardLoader shardLoader;
	private final ExecutorService executor = Executors.newFixedThreadPool(4);
	
	MarkovDatabaseImpl(String i, int d, String parentPath) {
		this.id = i;
		this.depth = d;
		this.path = parentPath + "/" + this.id;
		this.shardLoader = new ShardLoader(this.id, this.path, this.depth);
		this.shardCache = new ShardCache(this.id, 10, this.shardLoader.createStartShard());
		//creates a dummy spare shard that will never be accessed or used without first reloading it
		this.shardCache.initSpareShard(this.shardLoader.createShard("unused"));
	}
	
	MarkovDatabaseImpl(String i, int d, String parentPath, SaveType save) {
		this.id = i;
		this.depth = d;
		this.path = parentPath + "/" + this.id;
		this.shardLoader = new ShardLoader(this.id, this.path, this.depth, save);
		this.shardCache = new ShardCache(this.id, 10, this.shardLoader.createStartShard(), save);
		//creates a dummy spare shard that will never be accessed or used without first reloading it
		this.shardCache.initSpareShard(this.shardLoader.createShard("unused"));
	}
	
	MarkovDatabaseImpl(MarkovDatabaseBuilder builder) {
		this.id = builder.getId();
		this.path = builder.getParentPath() + "/" + this.id;
		this.depth = builder.getDepth();
		this.shardLoader = new ShardLoader(this.id, this.path, this.depth, builder.getSaveType());
		this.shardCache = new ShardCache(this.id, builder.getShardCacheSize(), this.shardLoader.createStartShard(), builder.getSaveType());
		//creates a dummy spare shard that will never be accessed or used without first reloading it
		//this.shardCache.initSpareShard(this.shardLoader.createShard("unused"));
	}
	
	
	/*
	 * TODO
	 * some kind of backup save/loading mechanic
	 * eg save every shard (ie everything inside this.path) to a zip or something
	 * and allow restoring from a zip or w/e
	 * should be a part of markovdb interface
	 */
	
	
	/*
	 * (non-Javadoc)
	 * @see my.cute.markov.MarkovDatabase#processLine(java.util.ArrayList)
	 */
	@Override
	public void processLine(List<String> words) {
		if(words.size() == 0) {
			logger.warn("attempt to process empty word array in " + this.toString());
			return;
		}
		
		//at least one element is present by above
		Bigram currentBigram = new Bigram(START_TOKEN, stripTokens(words.get(0)));
		String nextWord = "";
		int wordIndex = 1;
		while(wordIndex < words.size()) {
			nextWord = stripTokens(words.get(wordIndex));
			//this.addFollowingWordForBigram(currentBigram, nextWord);
			executor.execute(new AddFollowingWordTask(currentBigram, nextWord));
			currentBigram = new Bigram(currentBigram.getWord2(), nextWord);
			wordIndex++;
		}
		this.addFollowingWordForBigram(currentBigram, END_TOKEN);
	}
	
	private void addFollowingWordForBigram(Bigram bigram, String followingWord) {
		DatabaseShard shard = this.getShard(bigram);
		shard.addFollowingWord(bigram, followingWord);
	}
	
	private DatabaseShard getShard(Bigram bigram) {
		String prefix = this.getPrefix(bigram.getWord1());
		DatabaseShard shard = this.shardCache.get(prefix);
		if(shard != null) {
			return shard;
		} else {
			if(this.shardCache.isFull()) {
				shard = this.shardLoader.loadNewPrefix(this.shardCache.takeSpareShard(), prefix);
			} else {
				shard = this.shardLoader.createAndLoadShard(prefix);
			}
			return this.shardCache.add(shard);
		}
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
		
		while(index < depth) {
			prefix.append("A");
			index++;
		}
		
		//StringBuilder should be nonempty since above loop happened at least once
		return prefix.toString();
	}
	
	private StartDatabaseShard getStartShard() {
		return this.shardCache.getStartShard();
	}
	
	private String getRandomWeightedNextWord(Bigram bigram) {
		try {
			return this.getShard(bigram).getFollowingWord(bigram);
		} catch (IllegalArgumentException ex) {
			logger.warn("exception in trying to get next word: " + ex.getLocalizedMessage());
			return END_TOKEN;
		}
	}
	
	@Override
	public String generateLine() {
		return this.generateLine(this.getStartShard().getRandomWeightedStartWord());
	}
	
	@Override
	public String generateLine(String startingWord) {
		StringBuilder sb = new StringBuilder();
		Bigram currentBigram = new Bigram(START_TOKEN, startingWord);
		sb.append(currentBigram.getWord2());
		String nextWord = this.getRandomWeightedNextWord(currentBigram);
		int wordCount = 1;
		while(!nextWord.equals(END_TOKEN) && wordCount < MAX_WORDS_PER_LINE) {
			sb.append(" ");
			sb.append(nextWord);
			
			currentBigram = new Bigram(currentBigram.getWord2(), nextWord);
			nextWord = this.getRandomWeightedNextWord(currentBigram);
			wordCount++;
		}
		
		return sb.toString();
	}

	@Override
	public void save() {
		this.shardCache.save();
	}

	@Override
	public void load() {
		this.shardLoader.loadStartShard(this.shardCache.getStartShard());
	}

	@Override
	public void exportToTextFile() {
		/*
		 * create output file
		 * depth first search through all files in this.path
		 * if encounter directory, recursively search it
		 * if encounter file with .database, if it's ~start prefix then load as start shard
		 * 		else load as regular shard
		 * 		iterate through all its contents as stringbuilder and write to output file
		 */
		String timeStamp = new SimpleDateFormat("yyyyMMdd-HHmmss").format(Calendar.getInstance().getTime());
		String path = this.path + "/" + this.id + "_" + timeStamp + ".txt";
		for(File file : FileUtils.listFiles(new File(this.path), FileFilterUtils.suffixFileFilter(".database"), TrueFileFilter.TRUE)) {
			try {
				String databaseString = this.shardLoader.getShardFromFile(file).getDatabaseString();
				Files.write(Paths.get(path), databaseString.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
			} catch (IOException e) {
				logger.error("exception in trying to export " + this.toString() + " to text file (aborting): " + e.getLocalizedMessage());
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
	static String stripTokens(String input) {
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
