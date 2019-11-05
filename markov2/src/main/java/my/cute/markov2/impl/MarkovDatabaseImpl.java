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
	static final String START_TOKEN = "<_start>";
	static final String TOTAL_TOKEN = "<_total>";
	static final String END_TOKEN = "<_end>";
	private static final Map<String, String> tokenReplacements;
	static final String ZERO_DEPTH_PREFIX = "~database";
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
	
	MarkovDatabaseImpl(MarkovDatabaseBuilder builder) {
		this.id = builder.getId();
		this.path = builder.getParentPath() + "/" + this.id;
		this.depth = builder.getDepth();
		this.shardCache = new ShardCache(this.id, builder.getShardCacheSize(), builder.getDepth(), this.path, builder.getSaveType(),
				builder.getExecutorService());
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
		return prefix.toString();
	}
	
	private StartDatabaseShard getStartShard() {
		return this.shardCache.getStartShard();
	}
	
	@Override
	public String generateLine() {
		return this.generateLine(this.getStartShard().getRandomWeightedStartWord());
	}
	
	//TODO this also test it
	@Override
	public String generateLine(String startingWord) {
//		StringBuilder sb = new StringBuilder();
//		sb.append(startingWord);
//		int wordCount = 1;
//		CompletableFuture<Bigram> bigramFuture = CompletableFuture.completedFuture(new Bigram(START_TOKEN, startingWord));
//		CompletableFuture<String> nextWordFuture = this.getRandomWeightedNextWordFuture(bigramFuture);
//		while(this.shouldContinueGettingWords(nextWordFuture) && wordCount < MAX_WORDS_PER_LINE) {
//			sb.append(" ");
//			nextWordFuture.thenAccept(sb::append);
//			wordCount++;
//			
//			bigramFuture = nextWordFuture.thenCombine(bigramFuture, (word, bigram) -> new Bigram(bigram.getWord2(), word));
//			nextWordFuture = this.getRandomWeightedNextWordFuture(bigramFuture);
//		}
//		
//		return sb.toString();
		return "hello";
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
				String databaseString = this.shardCache.getDatabaseString(file);
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

	@Override
	public void load() {
		//does nothing
		//shouldnt be specified by interface?
	}

	@Override
	public int getSize() {
		//?
		return 0;
	}

}
