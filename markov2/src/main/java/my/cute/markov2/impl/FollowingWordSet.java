package my.cute.markov2.impl;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

public class FollowingWordSet implements Serializable, Iterable<Map.Entry<String, Integer>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static transient final Random RANDOM = new Random();
	//remember NEED TO SYNCHRONIZE MANUALLY ON THIS WHEN ITERATING
	private final LinkedHashMap<String, Integer> words;
	private int totalWordCount;
	
	FollowingWordSet() {
		this.totalWordCount = 0;
		this.words = new LinkedHashMap<String, Integer>(6);
	}
	
	public FollowingWordSet(String firstWord) {
		this();
		this.words.put(firstWord, 1);
		this.incrementTotal();
	}

	/*
	 * returns count associated with word after this add
	 * ie, returns 1 on first occurrence of a word, or a number greater than 1 in any other case
	 */
	int addWord(String word) {
		Integer count = this.words.get(word);
		//fix for put-if-absent as in http://dig.cs.illinois.edu/papers/checkThenAct.pdf
		//idk if im ever using this concurrently right now but good practice i guess
		if(count == null) {
			count = 0;
			Integer tmpCount = this.words.putIfAbsent(word, count);
			if(tmpCount != null) {
				count = tmpCount;
			}
		}
		count++;
		this.words.put(word, count);
		this.incrementTotal();
		return count;
	}
	
	/*
	 * returns a random word from the set, weighted according to their use count
	 */
	String getRandomWeightedWord() {
		String word = "";
		int count = RANDOM.nextInt(this.totalWordCount);
		/*
		 * need to synchronize on this if ever implementing multithreading at this level
		 */
		for(Map.Entry<String, Integer> entry : this.words.entrySet()) {
			if(count < entry.getValue()) {
				word = entry.getKey();
				break;
			} else {
				count -= entry.getValue();
			}
		}
		return word;
	}
	
	private void incrementTotal() {
		this.totalWordCount++;
	}

	public int getTotalWordCount() {
		return this.totalWordCount;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + totalWordCount;
		result = prime * result + ((words == null) ? 0 : words.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof FollowingWordSet)) {
			return false;
		}
		FollowingWordSet other = (FollowingWordSet) obj;
		if (totalWordCount != other.totalWordCount) {
			return false;
		}
		if (words == null) {
			if (other.words != null) {
				return false;
			}
		} else if (!words.equals(other.words)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("FollowingWordSet [words=");
		builder.append(words);
		builder.append(", totalWordCount=");
		builder.append(totalWordCount);
		builder.append("]");
		return builder.toString();
	}

	@Override
	public Iterator<Map.Entry<String, Integer>> iterator() {
		return this.words.entrySet().iterator();
	}

	
	
}
