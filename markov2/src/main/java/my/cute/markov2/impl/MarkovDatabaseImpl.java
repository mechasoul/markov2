package my.cute.markov2.impl;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import my.cute.markov2.MarkovDatabase;

public class MarkovDatabaseImpl implements MarkovDatabase {
	
	private static final Logger logger = LoggerFactory.getLogger(MarkovDatabaseImpl.class);
	
	private static final Pattern PUNCTUATION = Pattern.compile("\\p{Punct}");
	static final String START_TOKEN = MyStringPool.INSTANCE.intern("<_start>");
	static final String TOTAL_TOKEN = MyStringPool.INSTANCE.intern("<_total>");
	static final String END_TOKEN = MyStringPool.INSTANCE.intern("<_end>");
	private static final Map<String, String> tokenReplacements;
	static final String ZERO_DEPTH_PREFIX = "~database";
	static final String START_PREFIX = "~start";
	static final int MAX_WORDS_PER_LINE = 256;
	
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
		this.path = builder.getParentPath() + "/" + this.id;
		this.depth = builder.getDepth();
		this.shardCache = new ShardCache(this.id, builder.getShardCacheSize(), builder.getDepth(), this.path, builder.getSaveType(),
				builder.getExecutorService(), builder.getFixedCleanupThreshold());
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
		this.shardCache.addFollowingWord(this.getPrefix(bigram.getWord1()), bigram, MyStringPool.INSTANCE.intern(followingWord));
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
	
	public boolean removeLine(List<String> words) throws FollowingWordRemovalException {
		
		if(words.size() == 0) {
			logger.warn("attempt to remove empty word list in " + this.toString());
			return false;
		}
		
		//first ensure that entire word list exists in db
		
		//at least one element present by above so this get() never throws exception
		Bigram currentBigram = new Bigram(START_TOKEN, stripTokens(MyStringPool.INSTANCE.intern(words.get(0))));
		for(int i=1; i < words.size(); i++) {
			String followingWord = stripTokens(MyStringPool.INSTANCE.intern(words.get(i)));
			if(!this.getShard(currentBigram).contains(currentBigram, followingWord)) {
				return false;
			}
			currentBigram = new Bigram(currentBigram.getWord2(), followingWord);
		}
		if(!this.getShard(currentBigram).contains(currentBigram, END_TOKEN)) {
			return false;
		}
		
		//now perform the actual removals
		currentBigram = new Bigram(START_TOKEN, stripTokens(MyStringPool.INSTANCE.intern(words.get(0))));
		for(int i=1; i < words.size(); i++) {
			String followingWord = stripTokens(MyStringPool.INSTANCE.intern(words.get(i)));
			this.getShard(currentBigram).removeFollowingWord(currentBigram, followingWord);
			currentBigram = new Bigram(currentBigram.getWord2(), followingWord);
		}
		this.getShard(currentBigram).removeFollowingWord(currentBigram, END_TOKEN);
		return true;
	}

	@Override
	public void save() {
		this.shardCache.save();
	}

	@Override
	public void exportToTextFile() {
		/*
		 * builds human readable version of database
		 * checks all .database files in directory
		 * like everything else, breaks if outside sources modify db files
		 */
		String timeStamp = new SimpleDateFormat("yyyyMMdd-HHmmss").format(Calendar.getInstance().getTime());
		String path = this.path + "/" + this.id + "_" + timeStamp + ".txt";
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
			return MyStringPool.INSTANCE.intern(input);
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

	@Override
	public void load() {
		this.shardCache.load();
	}

}
