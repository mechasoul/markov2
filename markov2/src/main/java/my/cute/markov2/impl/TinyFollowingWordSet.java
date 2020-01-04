package my.cute.markov2.impl;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.nustaq.serialization.FSTObjectOutput;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;

class TinyFollowingWordSet implements FollowingWordSet, Serializable, Iterable<String> {
	
	static enum Pool {
		
		INSTANCE;
		
		private final Interner<TinyFollowingWordSet> interner = Interners.newWeakInterner();
		
		TinyFollowingWordSet intern(TinyFollowingWordSet sample) {
			return interner.intern(sample);
		}
	}

	private static final long serialVersionUID = 1L;
	
	static TinyFollowingWordSet of(String word) {
		return Pool.INSTANCE.intern(new TinyFollowingWordSet(ImmutableList.<String>builderWithExpectedSize(1).add(word).build()));
	}
	
	static TinyFollowingWordSet of(FollowingWordSet existing, String newWord) {
		return Pool.INSTANCE.intern(new TinyFollowingWordSet(
				ImmutableList.<String>builderWithExpectedSize(existing.size() + 1).addAll(existing.getWords()).add(newWord).build()));
	}
	
	static TinyFollowingWordSet of(ImmutableList<String> list) {
		return Pool.INSTANCE.intern(new TinyFollowingWordSet(list));
	}
	
	static TinyFollowingWordSet of(Collection<String> words) {
		return Pool.INSTANCE.intern(new TinyFollowingWordSet(
				ImmutableList.<String>builderWithExpectedSize(words.size()).addAll(words).build()));
	}
	
	static TinyFollowingWordSet remove(TinyFollowingWordSet set, String wordToRemove) {
		boolean shouldSkipWord = true;
		ImmutableList.Builder<String> builder = ImmutableList.<String>builderWithExpectedSize(set.size() - 1);
		for(String word : set) {
			if(word.equals(wordToRemove)) {
				//only skip including a single occurrence of the word
				if(!shouldSkipWord) {
					builder.add(word);
				}
				shouldSkipWord = false;
			} else {
				builder.add(word);
			}
		}
		return Pool.INSTANCE.intern(new TinyFollowingWordSet(builder.build()));
	}
	
	private final ImmutableList<String> words;
	private int hash;
	
	private TinyFollowingWordSet(ImmutableList<String> list) {
		this.words = list;
	}

	@Override
	public void addWord(String word) {
		throw new UnsupportedOperationException("can't add words to TinyFollowingWordSet! create new instance");
	}

	@Override
	public String getRandomWeightedWord() {
		return this.words.get(RANDOM.nextInt(this.size()));
	}

	@Override
	public int size() {
		return this.words.size();
	}

	@Override
	public boolean contains(String followingWord) {
		return this.words.contains(followingWord);
	}

	@Override
	public boolean contains(String followingWord, int count) {
		return (Collections.frequency(this.words, followingWord) >= count);
	}

	@Override
	public boolean remove(String followingWord) {
		throw new UnsupportedOperationException("can't remove words from TinyFollowingWordSet! create new instance");
	}

	@Override
	public boolean isEmpty() {
		return this.words.isEmpty();
	}

	@Override
	public Bigram getBigram() {
		throw new UnsupportedOperationException("no associated bigram for TinyFollowingWordSet instances");
	}

	@Override
	public String getId() {
		throw new UnsupportedOperationException("no associated id for TinyFollowingWordSet instances");
	}

	@Override
	public Type getType() {
		return FollowingWordSet.Type.TINY;
	}

	@Override
	public void writeToOutput(FSTObjectOutput out) throws IOException {
		out.writeInt(this.getType().getValue());
		out.writeInt(this.size());
		for(String word : this) {
			out.writeUTF(word);
		}
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("TinyFollowingWordSet [words=");
		builder.append(words);
		builder.append("]");
		return builder.toString();
	}

	@Override
	public String toStringPlain() {
		return this.words.toString();
	}

	@Override
	public Iterator<String> iterator() {
		return this.words.iterator();
	}

	@Override
	public int hashCode() {
		if(this.hash == 0) {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((words == null) ? 0 : words.hashCode());
			this.hash = result;
		}
		return this.hash;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!(obj instanceof TinyFollowingWordSet))
			return false;
		TinyFollowingWordSet other = (TinyFollowingWordSet) obj;
		if (words == null) {
			if (other.words != null)
				return false;
		} else if (!words.equals(other.words))
			return false;
		return true;
	}
	
	/*
	 * used for smallfollowingwordset constructor
	 */
	@Override
	public List<String> getWords() {
		return this.words;
	}

}
