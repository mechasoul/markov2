package my.cute.markov2;

import java.util.List;

public interface MarkovDatabase {

	/*
	 * processes a list of words into the database such that after this is called
	 * the database will reflect the use of the given words in given order
	 * words need to be nonempty, nonwhitespace
	 * words should also be intern()'d
	 */
	public void processLine(List<String> words);
	
	/*
	 * generates a message from the database with a weighted random starting word
	 */
	public String generateLine();
	
	/*
	 * generates a message from the database with the given starting word
	 */
	public String generateLine(String startingWord);
	
	/*
	 * saves database to disk
	 */
	public void save();
	
	/*
	 * loads database from disk
	 * must call load() after creating database and before using it
	 */
	public void load();
	
	/*
	 * exports database contents to an easily human-readable format
	 * time intensive for larger databases
	 * should really be separately threaded
	 */
	public void exportToTextFile();
	
	public String getId();
}
