package markov2;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TestUtils {
	
	private static final Logger logger = LoggerFactory.getLogger("TestLogger");
	
	static List<String> tokenize(String string) {
		return Arrays.asList(StringUtils.split(string, null));
	}
	
	static List<String> tokenizeForMarkov1(String string) {
		StringBuilder sb = new StringBuilder();
		sb.append("<[START]> ");
		sb.append(string);
		sb.append(" <[END]>");
		String str = sb.toString().replaceAll("\\|", "");
		return Arrays.asList(StringUtils.split(str, null));
	}
	
	static String getFormattedTime(long millis) {
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
	
	public static void log(String string) {
		logger.info(string);
	}
}
