package my.cute.markov2.impl;

import java.io.Serializable;
import gnu.trove.TCollections;
import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;

public class LargeFollowingWordSet implements FollowingWordSet, Serializable {
	
//	private class FollowingWordSetIterator implements Iterator<String> {
//
//		private int wordCount=0;
//		
//		private FollowingWordSetIterator() {
//		}
//		
//		@Override
//		public boolean hasNext() {
//			return this.wordCount < totalWordCount;
//		}
//
//		@Override
//		public String next() {
//			// TODO Auto-generated method stub
//			return null;
//		}
//		
//	}

	private static final long serialVersionUID = 1L;
	private final TObjectIntMap<String> words;
	private int totalWordCount;
	
	LargeFollowingWordSet() {
		this.totalWordCount = 0;
		this.words = TCollections.synchronizedMap(new TObjectIntHashMap<String>(7, 0.8f));
	}
	
	LargeFollowingWordSet(String firstWord) {
		this();
		this.words.put(firstWord, 1);
		this.incrementTotal();
	}
	
	LargeFollowingWordSet(SmallFollowingWordSet set) {
		this.totalWordCount = 0;
		this.words = TCollections.synchronizedMap(new TObjectIntHashMap<String>(set.size(), 0.8f));
		for(String word : set) {
			this.addWord(word);
		}
	}

	@Override
	public void addWord(String word) {
		this.words.adjustOrPutValue(word, 1, 1);
	}
	
	/*
	 * returns a random word from the set, weighted according to their use count
	 */
	@Override
	public String getRandomWeightedWord() {
		String chosenWord = "hello";
		int count = RANDOM.nextInt(this.totalWordCount);
		//need to synchronize when iterating over THashMap
		synchronized(this.words) {
			TObjectIntIterator<String> iterator = this.words.iterator();
			for(int i=0; i < this.words.size(); i++) {
				iterator.advance();
				if(count < iterator.value()) {
					chosenWord = iterator.key();
					break;
				} else {
					count -= iterator.value();
				}
			}
		}
		//should never return default string, since sum of word count over all entries should equal totalWordCount
		return chosenWord;
	}
	
	@Override
	public int size() {
		return this.totalWordCount;
	}
	
	private void incrementTotal() {
		this.totalWordCount++;
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
		if (!(obj instanceof LargeFollowingWordSet)) {
			return false;
		}
		LargeFollowingWordSet other = (LargeFollowingWordSet) obj;
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
	public String toStringPlain() {
		return words.toString();
	}
	
}
