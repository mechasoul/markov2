package markov2;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import my.cute.markov2.MarkovDatabase;
import my.cute.markov2.impl.Bigram;
import my.cute.markov2.impl.DatabaseShard;
import my.cute.markov2.impl.FollowingWordSet;
import my.cute.markov2.impl.MarkovDatabaseBuilder;
import my.cute.markov2.impl.MarkovDatabaseImpl;
import my.cute.markov2.impl.MyThreadPool;
import my.cute.markov2.impl.SaveType;
import my.cute.markov2.impl.ShardLoader;

public class Test {
	
	static int count=0;
	static long tempTime1=0;

	public static void main(String[] args) {
		String id = "shardtest";
		String path = "./test";
		String inPath = "./testinput.txt";
		File testFile = new File(inPath);
		
		MarkovDatabase db = new MarkovDatabaseBuilder(id, path)
			.depth(2)
			.shardCacheSize(64)
			.saveType(SaveType.JSON)
			.executorService(MyThreadPool.INSTANCE)
			.build();
		db.load();
		List<String> testLines = null;
		long time1 = System.currentTimeMillis();
		System.out.println("starting processing");
		
		try (Stream<String> lines = Files.lines(Paths.get(inPath), StandardCharsets.UTF_8)){
			tempTime1 = System.currentTimeMillis();
			lines
				.limit(100)
				.forEach(string -> 
			{
				if(StringUtils.isWhitespace(string)) return;
				db.processLine(tokenize(string));
				count++;
				if(count % 1000 == 0) {
					long tempTime2 = System.currentTimeMillis();
					double readRate = ShardLoader.loadTimer == 0 ? 0.0 : (((double)DatabaseShard.loadBytes) / ShardLoader.loadTimer) / 1000.0;
					double writeRate = DatabaseShard.saveTimer == 0 ? 0.0 : (((double)DatabaseShard.saveBytes) / DatabaseShard.saveTimer) / 1000.0;
					System.out.print(count + " - ");
					System.out.print(tempTime2 - tempTime1);
					System.out.print(", total i/o: " + (ShardLoader.loadTimer + DatabaseShard.saveTimer) + ", load time: " 
							+ ShardLoader.loadTimer + ", save time: " + DatabaseShard.saveTimer);
					System.out.println(", read: " + readRate + " MB/s, write: " + writeRate + " MB/s");
					tempTime1 = tempTime2;
					ShardLoader.loadTimer = 0;
					DatabaseShard.saveTimer = 0;
					DatabaseShard.saveBytes = 0;
					DatabaseShard.loadBytes = 0;
				}
			});
		} catch (IOException e1) {
			e1.printStackTrace();
		} 
		
		try {
			MyThreadPool.INSTANCE.awaitTermination(1, TimeUnit.MINUTES);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
	
		/*
		 * processing time for each batch of 1k messages seems to increase on average
		 * first few batches are ~3s, towards the end (20+ batches later) its over 10s
		 * why? hash problem? or jusut normal growth?
		 * also memory increases over time at an amount that seems too significant
		 * constant number of java.util.Vector increase over time?
		 * LinkedHashMap stuff is potentially problematic too
		 * that + $EntrySet + $LinkedEntrySet are increasing over time and im not sure if they should
		 * not constant increase like Vector tho
		 * cpu time dominated (>50%) by FileOutputStream.close() called by FileUtils.writeStringToFile()
		 * called by saveAsText() called by MyLinkedHashMap.removeEldestEntry() (cache)
		 */
		
		long time2 = System.currentTimeMillis();
		long netTime = time2 - time1;
		System.out.println("processing finished");
		long time3 = System.currentTimeMillis();
		db.save();
		System.out.println("save took " + (System.currentTimeMillis() - time3) + "ms");
		StringBuilder sb = new StringBuilder();
		sb.append("total processing time: ");
		sb.append(getFormattedTime(netTime));
		sb.append("\r\n");
		sb.append("total lines: " + count);
		sb.append("\r\n");
		sb.append("average time: ");
		long avgTime = netTime / count;
		sb.append(avgTime + "ms");
		try {
			FileUtils.writeStringToFile(new File("testoutput.txt"), sb.toString(), StandardCharsets.UTF_8, false);
			System.out.println(sb.toString());
		} catch (IOException e) {
			System.out.println("??");
			e.printStackTrace();
		}
		System.out.println("beginning export to txt");
		time1 = System.currentTimeMillis();
		db.exportToTextFile();
		System.out.println("export finished in " + getFormattedTime(System.currentTimeMillis() - time1));
//		DatabaseShard shard = new DatabaseShard(id, MarkovDatabaseImpl.ZERO_DEPTH_PREFIX, path);
//		shard.addFollowingWord(new Bigram("im","very"), "gay");
//		shard.addFollowingWord(new Bigram("im","very"), "cute");
//		shard.addFollowingWord(new Bigram("im","not"), "cute");
//		shard.saveAsText();
//		DatabaseShard shard2 = new DatabaseShard(id, MarkovDatabaseImpl.ZERO_DEPTH_PREFIX, path);
//		shard2.loadFromText();
//		System.out.println(shard.toStringFull());
//		System.out.println(shard2.toStringFull());
//		Gson gson = new GsonBuilder()
//			.enableComplexMapKeySerialization()
//			.create();
//		Map<Bigram, FollowingWordSet> map = new LinkedHashMap<Bigram, FollowingWordSet>(6);
//		map.put(new Bigram("hi","hello"), new FollowingWordSet("aa"));
//		System.out.println(map);
//		Type type = new TypeToken<LinkedHashMap<Bigram, FollowingWordSet>>() {}.getType();
//		String json = gson.toJson(map, type);
//		System.out.println(json);
//		File file = new File(path);
//		try {
//			FileUtils.writeStringToFile(file, json, StandardCharsets.UTF_8, false);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		String json2="";
//		try {
//			json2 = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		Map<Bigram, FollowingWordSet> map2 = gson.fromJson(json2, type);
//		System.out.println(map2);
	}
	
	private static List<String> tokenize(String string) {
		return Arrays.asList(StringUtils.split(string, null));
	}
	
	private static String getFormattedTime(long millis) {
		long minutes = TimeUnit.MILLISECONDS.toMinutes(millis);
		millis -= TimeUnit.MINUTES.toMillis(minutes);
		long seconds = TimeUnit.MILLISECONDS.toSeconds(millis);
		millis -= TimeUnit.SECONDS.toMillis(seconds);
		StringBuilder sb = new StringBuilder();
		sb.append(minutes);
		sb.append(":");
		sb.append(seconds);
		sb.append(".");
		sb.append(millis);
		return sb.toString();
	}
}
