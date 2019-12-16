package my.cute.markov2.impl;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Random;

import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StartDatabaseShard extends DatabaseShard {

	@SuppressWarnings("unused")
	private static final Logger logger = LoggerFactory.getLogger(StartDatabaseShard.class);
	private static transient final Random RANDOM = new Random();
	
	private int totalCount;
	
	StartDatabaseShard(String id, String p, String parentPath) {
		super(id, p, parentPath);
		this.totalCount = 0;
	}
	
	@Override
	public boolean addFollowingWord(Bigram bigram, String followingWord) {
		boolean result = super.addFollowingWord(bigram, followingWord);
		this.totalCount++;
		return result;
	}
	
	/*
	 * gets a random word used to start a message, weighted by word use
	 * in the start shard, all bigrams have word1 = START_TOKEN, and word2 = actual starting word
	 * O(n) but its expensive on memory to get faster than that and memory is more of a premium
	 * throws illegalargumentexception if totalCount == 0 (empty database)
	 */
	String getRandomWeightedStartWord() throws IllegalArgumentException {
		String word = "hello";
		int count = RANDOM.nextInt(this.totalCount);
		for(Map.Entry<Bigram, FollowingWordSet> entry : this.database.entrySet()) {
			if(count < entry.getValue().size()) {
				word = entry.getKey().getWord2();
				break;
			} else {
				count -= entry.getValue().size();
			}
		}
		
		/* sum of totalWordCount over all entries should be the same as this.totalCount
		 * so the only way this returns default string "hello" is if db is empty
		 */
		return word;
	}
	
	@Override
	void saveAsText() throws IOException {
		StringBuilder sb = new StringBuilder(GSON.toJson(this.database, DATABASE_TYPE));
		sb.append("\r\n");
		sb.append(this.totalCount);
		Files.write(this.path, sb.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
	}
	
	
	@Override
	void loadFromText() throws FileNotFoundException, NoSuchFileException, IOException {
		try (BufferedReader reader = Files.newBufferedReader(this.path, StandardCharsets.UTF_8)) {
			this.database = GSON.fromJson(reader.readLine(), DATABASE_TYPE);
			this.totalCount = Integer.parseInt(reader.readLine());
		}
	}
	
	@Override
	void saveAsObject() throws IOException {
		FileOutputStream fileOutputStream = new FileOutputStream(this.path.toString());
		FSTObjectOutput out = CONF.getObjectOutput(fileOutputStream);
		out.writeObject(this.database, DatabaseWrapper.class);
		out.writeInt(this.totalCount);
		out.flush();
		fileOutputStream.close();
	}
	
	@Override
	void loadFromObject() throws Exception {
		FileInputStream fileInputStream = new FileInputStream(this.path.toString());
		FSTObjectInput in = CONF.getObjectInput(fileInputStream);
		this.database = (DatabaseWrapper) in.readObject(DatabaseWrapper.class);
		this.totalCount = in.readInt();
		fileInputStream.close();
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("StartDatabaseShard [totalCount=");
		builder.append(totalCount);
		builder.append(", parentDatabaseId=");
		builder.append(parentDatabaseId);
		builder.append(", prefix=");
		builder.append(prefix);
		builder.append("]");
		return builder.toString();
	}

	
}
