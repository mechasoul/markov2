package my.cute.markov2.impl;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.nustaq.serialization.FSTObjectOutput;
import org.nustaq.serialization.annotations.Flat;

/*
 * FollowingWordSet implementation used for less commonly used bigrams
 * set is maintained as a simple arraylist and each time a word is used it's
 * added to the list. this is a very lightweight implementation for lists
 * with few repeated words - as the set grows words tend to be repeated more
 * frequently, so LargeFollowingWordSet is used in that case
 * due to the backing arraylist, getRandomWeightedWord() is O(1) and remove()
 * and contains() are O(n), though the size limitations of this class mean 
 * that this shouldnt really be a concern
 */
@Flat
class SmallFollowingWordSet implements FollowingWordSet, Serializable, Iterable<String> {
	
	private static final long serialVersionUID = 1L;
	private final List<String> words;
	/*
	 * the bigram this fws corresponds to
	 */
	private final Bigram bigram;
	/*
	 * the id for the server this fws (and its corresponding bigram) belong to
	 */
	private final String parentDatabaseId;
	
	SmallFollowingWordSet(String firstWord, Bigram bigram, String id) {
		this.words = Collections.synchronizedList(new ArrayList<String>(1));
		this.bigram = bigram;
		this.parentDatabaseId = id;
		this.addWord(firstWord);
	}
	
	SmallFollowingWordSet(List<String> list, Bigram bigram, String id) {
		this.words = list;
		this.bigram = bigram;
		this.parentDatabaseId = id;
	}
	
	SmallFollowingWordSet(FollowingWordSet set, String newWord, Bigram bigram, String id) {
		this.words = Collections.synchronizedList(new ArrayList<String>(set.getWords()));
		this.bigram = bigram;
		this.parentDatabaseId = id;
		this.addWord(newWord);
		
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
	 * O(n) for this implementation of FollowingWordSet
	 */
	@Override
	public boolean contains(String followingWord) {
		synchronized(this.words) {
			return this.words.contains(followingWord);
		}
	}
	
	@Override
	public boolean contains(String followingWord, int count) {
		synchronized(this.words) {
			return (Collections.frequency(this.words, followingWord) >= count);
		}
	}

	/*
	 * O(n) for this implementation of FollowingWordSet
	 */
	@Override
	public boolean remove(String followingWord) {
		synchronized(this.words) {
			return this.words.remove(followingWord);
		}
	}
	
	@Override
	public boolean isEmpty() {
		return this.words.isEmpty();
	}
	
	@Override
	public Bigram getBigram() {
		return this.bigram;
	}

	@Override
	public String getId() {
		return this.parentDatabaseId;
	}

	/*
	 * must synchronize manually on this when iterating
	 */
	@Override
	public Iterator<String> iterator() {
		return this.words.iterator();
	}

	/*
	 * for synchronizing on
	 */
	@Override
	public List<String> getWords() {
		return this.words;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((bigram == null) ? 0 : bigram.hashCode());
		result = prime * result + ((parentDatabaseId == null) ? 0 : parentDatabaseId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!(obj instanceof SmallFollowingWordSet))
			return false;
		SmallFollowingWordSet other = (SmallFollowingWordSet) obj;
		if (bigram == null) {
			if (other.bigram != null)
				return false;
		} else if (!bigram.equals(other.bigram))
			return false;
		if (parentDatabaseId == null) {
			if (other.parentDatabaseId != null)
				return false;
		} else if (!parentDatabaseId.equals(other.parentDatabaseId))
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

	@Override
	public Type getType() {
		return FollowingWordSet.Type.SMALL;
	}

	@Override
	public void writeToOutput(FSTObjectOutput out) throws IOException {
		
		out.writeInt(this.getType().getValue());
		out.writeInt(this.size());
		synchronized(this.words) {
			for(String word : this) {
				out.writeUTF(word);
			}
		}
	}

	
}
