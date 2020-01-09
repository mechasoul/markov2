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
import java.util.concurrent.ExecutorService;
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
import my.cute.markov2.impl.LargeFollowingWordSet;
import my.cute.markov2.impl.MarkovDatabaseBuilder;
import my.cute.markov2.impl.MarkovDatabaseImpl;
import my.cute.markov2.impl.MyThreadPool;
import my.cute.markov2.impl.SaveType;
import my.cute.markov2.impl.ShardLoader;

public class Test {
	
	static int count=0;
	static long tempTime1=0;

	public static void main(String[] args) {
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		String id = "shardtest";
		String path = "./test";
		String inPath = "./testinput.txt";
		File testFile = new File(inPath);
		ForkJoinPool exec = ForkJoinPool.commonPool();
		
		MarkovDatabase db = new MarkovDatabaseBuilder(id, path)
			.shardCacheSize(0)
			.saveType(SaveType.SERIALIZE)
			.executorService(exec)
			.fixedCleanupThreshold(100)
			.build();
		db.load();
		List<String> testLines = null;
		long time1 = System.currentTimeMillis();
		System.out.println("starting processing");
		
		try (Stream<String> lines = Files.lines(Paths.get(inPath), StandardCharsets.UTF_8)){
			tempTime1 = System.currentTimeMillis();
			lines
				.limit(100000)
				.forEach(string -> 
			{
				if(StringUtils.isWhitespace(string)) return;
				db.processLine(TestUtils.tokenize(string));
				count++;
				if(count % 1000 == 0) {
					long tempTime2 = System.currentTimeMillis();
					System.out.print(count + " - ");
					System.out.println(tempTime2 - tempTime1);
//					if((tempTime2 - tempTime1) > 20000) {
//						MarkovDatabaseImpl.LOG = true;
//					} else {
//						MarkovDatabaseImpl.LOG = false;
//					}
					tempTime1 = tempTime2;
				}
//				if(count == 1346000) {
//					try {
//						Thread.sleep(4000);
//					} catch (InterruptedException e) {
//						e.printStackTrace();
//					}
//				}
			});
		} catch (IOException e1) {
			e1.printStackTrace();
		} 
		
		try {
			exec.awaitQuiescence(4, TimeUnit.MINUTES);
			exec.shutdown();
			exec.awaitTermination(4, TimeUnit.MINUTES);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}

		long time2 = System.currentTimeMillis();
		long netTime = time2 - time1;
		System.out.println("processing finished");
		long time3 = System.currentTimeMillis();
		db.save();
		System.out.println("save took " + (System.currentTimeMillis() - time3) + "ms");
		StringBuilder sb = new StringBuilder();
		sb.append("total processing time: ");
		sb.append(TestUtils.getFormattedTime(netTime));
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
		System.out.println("export finished in " + TestUtils.getFormattedTime(System.currentTimeMillis() - time1));
		//barf
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
}
