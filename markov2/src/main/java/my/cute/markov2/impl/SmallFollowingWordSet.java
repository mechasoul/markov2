package my.cute.markov2.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

class SmallFollowingWordSet implements FollowingWordSet, Serializable, Iterable<String> {

	private static final long serialVersionUID = 1L;
	private final List<String> words;
	
	SmallFollowingWordSet() {
		this.words = Collections.synchronizedList(new ArrayList<String>(1));
	}
	
	SmallFollowingWordSet(String firstWord) {
		this.words = Collections.synchronizedList(new ArrayList<String>(1));
		this.addWord(firstWord);
	}
	
	@Override
	public void addWord(String word) {
		this.words.add(word);
	}

	@Override
	public String getRandomWeightedWord() {
		return this.words.get(RANDOM.nextInt(this.words.size()));
	}

	@Override
	public int size() {
		return this.words.size();
	}

	/*
	 * must synchronize manually on this when iterating
	 */
	@Override
	public Iterator<String> iterator() {
		return this.words.iterator();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((words == null) ? 0 : words.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!(obj instanceof SmallFollowingWordSet))
			return false;
		SmallFollowingWordSet other = (SmallFollowingWordSet) obj;
		if (words == null) {
			if (other.words != null)
				return false;
		} else if (!words.equals(other.words))
			return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("SmallFollowingWordSet [words=");
		builder.append(words);
		builder.append("]");
		return builder.toString();
	}
	
	@Override
	public String toStringPlain() {
		return words.toString();
	}

}
