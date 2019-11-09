package my.cute.markov2.impl;

import java.util.Random;

interface FollowingWordSet {

	static final Random RANDOM = new Random();
	/*
	 * adds an instance of the given word to the set
	 */
	public void addWord(String word);
	
	/*
	 * gets a randomly chosen word from the set, weighted by frequency
	 */
	public String getRandomWeightedWord();
	
	/*
	 * gets size of the set (equivalent to the number of times addWord() has been called)
	 */
	public int size();
	
	/*
	 * returns a minimal String representation of the set's contents
	 */
	public String toStringPlain();
}
