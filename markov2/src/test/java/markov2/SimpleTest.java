package markov2;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;

import my.cute.markov2.MarkovDatabase;
import my.cute.markov2.impl.MarkovDatabaseBuilder;
import my.cute.markov2.impl.SaveType;

public class SimpleTest {

	public static void main(String[] args) {
		
		String id = "smalltest";
		String path = "./test";
		String inPath = "./testinput.txt";
		File testFile = new File(inPath);
		
		MarkovDatabase db = new MarkovDatabaseBuilder(id, path)
			.depth(1)
			.shardCacheSize(1)
			.saveType(SaveType.JSON)
			//.executorService(MyThreadPool.INSTANCE)
			.build();
		db.load();
		List<String> testLines = null;
		long time1 = System.currentTimeMillis();
		System.out.println("starting processing");
		db.processLine(tokenize("hello hi how are you how are hello hi hello"));
		db.processLine(tokenize("i am well how are you doing today just wondering id like to know"));
		db.save();
		System.out.println("finished in " + getFormattedTime(System.currentTimeMillis() - time1));
		db.exportToTextFile();
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
