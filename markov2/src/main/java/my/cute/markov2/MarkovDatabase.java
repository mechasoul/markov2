package my.cute.markov2;

import java.util.List;

import my.cute.markov2.impl.FollowingWordRemovalException;

public interface MarkovDatabase {

	/*
	 * processes a list of words into the database such that after this is called
	 * the database will reflect the use of the given words in given order
	 * words need to be nonempty, nonwhitespace
	 * words will be intern'd with weak references via guava interner
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
	 * removes an occurrence of the given line as a list of words
	 * could be used for eg removing lines that are old to cap db size
	 * DANGER should only use this on exact lines that are known to have been
	 * processed in the past! if this is called with arbitrary lines it could
	 * lead to inconsistent db state. implementation should check ahead of time
	 * to make sure that there's an occurrence of each bigram -> word in the db
	 * already before doing anything, to avoid running into problems partway 
	 * through and leaving db in inconsistent state
	 * 
	 * consequently this will probably be slower than processLine and should be
	 * used sparingly. extra caution should also be given to multithreaded 
	 * environments. probably best used in some kind of maintenance state where
	 * the db can be locked for some time while lines are removed?
	 * 
	 * returns true if the line was successfully removed, and false if the entire
	 * line wasn't found in the database
	 * 
	 * throws FollowingWordRemovalException if the line was found to be in the database
	 * but some bigram->word was missing when actually removed (indicating probably
	 * some concurrency problem)
	 */
	public boolean removeLine(List<String> words) throws FollowingWordRemovalException;
	
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
