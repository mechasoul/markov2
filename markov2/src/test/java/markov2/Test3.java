package markov2;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class Test3 {

	static String jsonPath = "./testjson.txt";
	static String outputPath = "./testoutput1.txt";
	
	public static void main(String[] args) {
		
		File file = new File(jsonPath);
		System.out.println(file.getName());
		System.out.println(file.getName().split("\\.")[0]);
		
		try (Stream<String> lines = Files.lines(Paths.get("./in1.txt"), StandardCharsets.UTF_8)) {
			StringBuilder sb = new StringBuilder();
			lines.sorted().forEach(line ->
			{
				sb.append(line);
				sb.append("\r\n");
			});
			FileUtils.writeStringToFile(new File("./out1.txt"), sb.toString(), StandardCharsets.UTF_8, false);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
