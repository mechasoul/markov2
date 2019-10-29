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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class Test3 {

	static String jsonPath = "./testjson.txt";
	static String outputPath = "./testoutput1.txt";
	
	public static void main(String[] args) {
		
		File file = new File(jsonPath);
		System.out.println(file.getName());
		System.out.println(file.getName().split("\\.")[0]);
	}

}
