package markov2;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;

import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import my.cute.markov2.MarkovDatabase;
import my.cute.markov2.impl.MarkovDatabaseBuilder;
import my.cute.markov2.impl.SaveType;

public class SimpleTest {

	public static void main(String[] args) {
		
		TObjectIntMap<String> map = new TObjectIntHashMap<String>(7, 0.8f);
		System.out.println(map);
		map.adjustOrPutValue("dicks", 1, 1);
		System.out.println(map);
		map.adjustOrPutValue("dicks", 1, 1);
		System.out.println(map);
		map.adjustOrPutValue("hello", 1, 1);
		System.out.println(map);
		map.adjustOrPutValue("dicks", 1, 1);
		System.out.println(map);
		map.adjustOrPutValue("smile", 1, 1);
		System.out.println(map);
		map.adjustOrPutValue("dicks", 1, 1);
		System.out.println(map);
		TObjectIntIterator<String> it = map.iterator();
		for(int i=0; i < map.size(); i++) {
			it.advance();
			System.out.println(it.key());
		}
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
