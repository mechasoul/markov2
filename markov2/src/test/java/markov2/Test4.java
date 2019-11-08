package markov2;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import my.cute.markov.MarkovDatabase;

public class Test4 {

	static int count;
	static long tempTime1;
	
	public static void main(String[] args) {
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		String inPath = "./testinput.txt";
		MarkovDatabase db = new MarkovDatabase("0", "oldtest", 10, 7, true);
		List<String> testLines = null;
		long time1 = System.currentTimeMillis();
		System.out.println("starting processing");

		try (Stream<String> lines = Files.lines(Paths.get(inPath), StandardCharsets.UTF_8)){
			tempTime1 = System.currentTimeMillis();
			lines
			//.limit(100000)
			.forEach(string -> 
			{
				if(StringUtils.isWhitespace(string)) return;
				db.process(TestUtils.tokenizeForMarkov1(string));
				count++;
				if(count % 1000 == 0) {
					long tempTime2 = System.currentTimeMillis();
					System.out.print(count + " - ");
					System.out.println(tempTime2 - tempTime1);
					tempTime1 = tempTime2;
				}
			});
		} catch (IOException e1) {
			e1.printStackTrace();
		} 

		long time2 = System.currentTimeMillis();
		long netTime = time2 - time1;
		System.out.println("processing finished");
		long time3 = System.currentTimeMillis();
		db.saveDatabase();
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

	}

}
