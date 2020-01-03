package my.cute.markov2;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import my.cute.markov2.exceptions.FollowingWordRemovalException;

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
	 * used to check if the database has processed the given line (as a list of words)
	 * not sure why this would really be needed for general use but might as well
	 * since it's used for removeLine()
	 * 
	 * note that because of the limited memory of markov chains it's possible for
	 * this to return true even if the exact given line has never been processed,
	 * if multiple other lines have been processed that result in the db containing
	 * the same data as it'd have from processing this line. this should basically
	 * never matter though
	 * 
	 * returns true if the given line has been processed in the database (ie, every
	 * bigram -> word contained within the line exists in the database as many times
	 * as it occurs in the line)
	 */
	public boolean contains(List<String> words);
	
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
	 * consequently this will be slower than processLine and should be
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
	 * used to save database to a backup (.zip)
	 * takes a name used to identify the backup. if a backup already exists with the given
	 * name, it will be overwritten
	 * returns a Path to the created database backup file
	 */
	public Path saveBackup(String backupName) throws IOException;
	
	/*
	 * used to load database from a backup (.zip)
	 */
	public void loadBackup(String backupName) throws FileNotFoundException, IOException;
	
	/*
	 * used to delete a backup with the given name
	 * returns true if a backup was deleted as a result of this call, false otherwise
	 */
	public boolean deleteBackup(String backupName) throws IOException;
	
	/*
	 * exports database contents to an easily human-readable format
	 * time intensive for larger databases
	 * should really be separately threaded
	 */
	public void exportToTextFile();
	
	public String getId();
	
}
