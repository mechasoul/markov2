package markov2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class Test2 {

	private static class MyLinkedHashMap extends LinkedHashMap<String, String> {
		
		private static final long serialVersionUID = 1L;
		private final int maxCapacity;
		
		MyLinkedHashMap(int initialCapacity, float loadFactor, boolean accessMethod, int m) {
			super(initialCapacity, loadFactor, accessMethod);
			this.maxCapacity = m;
		}
		
		@Override
		protected boolean removeEldestEntry(Map.Entry<String, String> eldestEntry) {
			return this.size() > this.maxCapacity;
		}
	}
	
	private static class PerformanceTimer {
		private long min = Long.MAX_VALUE;
		private long max = 0;
		private long total = 0;
		private int trials = 0;
		private final String descriptor;
		
		public PerformanceTimer(String d) { this.descriptor = d; }
		
		public long getMin() { return min; }
		public long getMax() { return max; }
		public double getAverage() { return ((double)total) / trials; }
		
		public void addTrial(long time) {
			if(time < min) min = time;
			if(time > max) max = time;
			total += time;
			trials++;
		}
		
		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder(descriptor);
			sb.append(" min: ");
			sb.append(min);
			sb.append(", max: ");
			sb.append(max);
			sb.append(", average: ");
			sb.append(getAverage());
			return sb.toString();
		}
	}
	
	static String inputPath = "./testinput.txt";
	static String jsonPath = "./testjson.txt";
	static String outputPath = "./testoutput.txt";
	
	static final int NUM_READS = 4;
	static final int NUM_WRITES = 3;
	static final int NUM_TESTS = 10;
	
	static PerformanceTimer[] readTimes = new PerformanceTimer[NUM_READS];
	static PerformanceTimer[] writeTimes = new PerformanceTimer[NUM_WRITES];
	
	public static void main(String[] args) {
//		int capacity = 10;
//		MyLinkedHashMap map = new MyLinkedHashMap(capacity * 4 / 3, 1f, true, capacity);
//		for(long i=0; i < 1E10; i++) {
//			map.put("key_" + i, "value_" + i);
//			if(i % 1000000 == 0) {
//				System.out.println(i);
//			}
//		}
//		for(int i=0; i < readTimes.length; i++) {
//			readTimes[i] = new PerformanceTimer();
//		}
//		for(int i=0; i < writeTimes.length; i++) {
//			writeTimes[i] = new PerformanceTimer();
//		}
		readTimes[0] = new PerformanceTimer("FileUtils.readFileToString()");
		readTimes[1] = new PerformanceTimer("Files.lines() stream");
		readTimes[2] = new PerformanceTimer("Files.readAllBytes()");
		readTimes[3] = new PerformanceTimer("Files.newBufferedReader()");
		
		writeTimes[0] = new PerformanceTimer("Files.write()");
		writeTimes[1] = new PerformanceTimer("FileUtils.writeStringToFile()");
		writeTimes[2] = new PerformanceTimer("Files.newBufferedWriter()");
		
		
		for(int i=0; i < NUM_TESTS; i++) {
			testRead();
			testWrite();
			System.out.println("trial " + i + " done");
		}
		
		for(int i=0; i < readTimes.length; i++) {
			System.out.println(readTimes[i]);
		}
		for(int i=0; i < writeTimes.length; i++) {
			System.out.println(writeTimes[i]);
		}
	}
	
	static void testWrite() {
		File testFile = new File(inputPath);
		long time1;
		List<String> testLines = null;
		try {
			testLines = FileUtils.readLines(testFile, StandardCharsets.UTF_8);
		} catch (IOException e) {
			System.out.println("??");
			e.printStackTrace();
			System.exit(0);
		}
		Type type = new TypeToken<List<String>>() {}.getType();
		Gson gson = new Gson();
		String json = "";
		//warmup start
		time1 = System.currentTimeMillis();
		json = gson.toJson(testLines, type);
		try {
			FileUtils.writeStringToFile(new File("./testoutputwarm.txt"), json, StandardCharsets.UTF_8);
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println((System.currentTimeMillis() - time1) + " warmup");
		//warmuup end
		time1 = System.currentTimeMillis();
		json = gson.toJson(testLines, type);
		try {
			Files.write(Paths.get("./testoutput1.txt"), json.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
		} catch (IOException e) {
			e.printStackTrace();
		}
		writeTimes[0].addTrial(System.currentTimeMillis() - time1);
		time1 = System.currentTimeMillis();
		json = gson.toJson(testLines, type);
		try {
			FileUtils.writeStringToFile(new File("./testoutput2.txt"), json, StandardCharsets.UTF_8);
		} catch (IOException e) {
			e.printStackTrace();
		}
		writeTimes[1].addTrial(System.currentTimeMillis() - time1);
		time1 = System.currentTimeMillis();
		json = gson.toJson(testLines, type);
		try (BufferedWriter writer = Files.newBufferedWriter(Paths.get("./testoutput3.txt"), StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
			writer.write(json);
		} catch (IOException e) {
			e.printStackTrace();
		}
		writeTimes[2].addTrial(System.currentTimeMillis() - time1);
	}
	
	static void testRead() {
		File testFile = new File(inputPath);
		long time1 = System.currentTimeMillis();
		List<String> testLines = null;
		try {
			testLines = FileUtils.readLines(testFile, StandardCharsets.UTF_8);
		} catch (IOException e) {
			System.out.println("??");
			e.printStackTrace();
			System.exit(0);
		}
		Type type = new TypeToken<List<String>>() {}.getType();
		Gson gson = new Gson();
		String json="";
		List<String> testLines2 = null;
		//warmup start
		time1 = System.currentTimeMillis();
		try (BufferedReader reader = Files.newBufferedReader(Paths.get(jsonPath), StandardCharsets.UTF_8)) {
			json = reader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		testLines2 = gson.fromJson(json, type);
		System.out.println((System.currentTimeMillis() - time1) + " " + testLines.equals(testLines2) + " warmup");
		//warmup end
		time1 = System.currentTimeMillis();
		try {
			json = FileUtils.readFileToString(new File(jsonPath), StandardCharsets.UTF_8);
		} catch (IOException e) {
			e.printStackTrace();
		}
		testLines2 = gson.fromJson(json, type);
		readTimes[0].addTrial(System.currentTimeMillis() - time1);
		if(!testLines.equals(testLines2)) System.out.println("thing wasnt equal emergency");
		time1 = System.currentTimeMillis();
		StringBuilder jsonBuilder = new StringBuilder();
		try(Stream<String> lines = Files.lines(Paths.get(jsonPath), StandardCharsets.UTF_8)) {
			lines.forEach(jsonBuilder::append);
		} catch (IOException e) {
			e.printStackTrace();
		}
		testLines2 = gson.fromJson(jsonBuilder.toString(), type);
		readTimes[1].addTrial(System.currentTimeMillis() - time1);
		if(!testLines.equals(testLines2)) System.out.println("thing wasnt equal emergency");
		time1 = System.currentTimeMillis();
		try {
			json = new String(Files.readAllBytes(Paths.get(jsonPath)), StandardCharsets.UTF_8);
		} catch (IOException e) {
			e.printStackTrace();
		}
		testLines2 = gson.fromJson(json, type);
		readTimes[2].addTrial(System.currentTimeMillis() - time1);
		if(!testLines.equals(testLines2)) System.out.println("thing wasnt equal emergency");
		time1 = System.currentTimeMillis();
		try (BufferedReader reader = Files.newBufferedReader(Paths.get(jsonPath), StandardCharsets.UTF_8)) {
			json = reader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		testLines2 = gson.fromJson(json, type);
		readTimes[3].addTrial(System.currentTimeMillis() - time1);
		if(!testLines.equals(testLines2)) System.out.println("thing wasnt equal emergency");
	}

	static void write(String path, String string) {
		try {
			Files.write(Paths.get(path), string.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
		} catch (IOException e) {
			System.out.println("??");
			e.printStackTrace();
			System.exit(0);
		}
	}
}
