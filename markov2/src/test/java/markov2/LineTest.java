package markov2;

import java.io.File;
import java.util.concurrent.ForkJoinPool;

import my.cute.markov2.MarkovDatabase;
import my.cute.markov2.impl.MarkovDatabaseBuilder;
import my.cute.markov2.impl.SaveType;

public class LineTest {

	public static void main(String[] args) {
		try {
			Thread.sleep(7000);
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
			.executorService(exec)
			.fixedCleanupThreshold(100)
			.build();
		db.load();

//		MarkovDatabase db = new MarkovDatabase("0", "oldtest", 10, 20, true);
//		OutputHandler output = new OutputHandler(db);
		
		final int NUM_LINES = 100000;
		final int SEGMENT = 1000;
		long startTime = System.currentTimeMillis();
		long time1 = startTime;
		for(int i=0; i < NUM_LINES; i++) {
			if(i > 0 && i % SEGMENT == 0) {
				long time2 = System.currentTimeMillis();
				StringBuilder sb = new StringBuilder();
				sb.append(i);
				sb.append(" - total: ");
				sb.append(TestUtils.getFormattedTime(time2 - time1));
				sb.append(", avg: ");
				sb.append((double)(time2 - time1) / SEGMENT);
				sb.append("ms");
				TestUtils.log(sb.toString());
				time1 = time2;
			}
			String line = db.generateLine();
			if(i > 0 && i % 10 == 0) {
				System.out.println(line);
			}
		}
		TestUtils.log("total: " + TestUtils.getFormattedTime(System.currentTimeMillis() - startTime) 
				+ ", avg: " + ((double)(System.currentTimeMillis() - startTime)/NUM_LINES) + "ms");
	}

}
