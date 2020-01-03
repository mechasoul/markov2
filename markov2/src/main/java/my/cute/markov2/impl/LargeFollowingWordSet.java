package my.cute.markov2.impl;

import java.io.IOException;
import java.io.Serializable;
import org.nustaq.serialization.FSTObjectOutput;
import org.nustaq.serialization.annotations.Flat;

import gnu.trove.TCollections;
import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;

@Flat
public class LargeFollowingWordSet implements FollowingWordSet, Serializable {
	
//	/*
//	 * extend trove procedure to rethrow exception outside of lambda
//	 * trove maps are best iterated on by a .forEachEntry() lambda method because
//	 * of something about the way theyre stored, and we need to iterate on map
//	 * when writing all of its values to output during serializer write, so we
//	 * do this in order to get any ioexception thrown during the forEachEntry out
//	 * of the lambda so it can be thrown from the serializer's write method as per
//	 * usual
//	 * janky but the best way i can think of to do this due to the limitations with
//	 * lambdas/checked exceptions
//	 */
//	@FunctionalInterface
//	private static interface IOProcedure<E> extends TObjectIntProcedure<E> {
//		
//		@Override
//		default boolean execute(E e, int b) {
//			try {
//				return executeIO(e, b);
//			} catch (IOException ex) {
//				throw new UncheckedIOException(ex);
//			}
//		}
//		
//		boolean executeIO(E e, int b) throws IOException;
//	}
	
	private static final long serialVersionUID = 1L;
	private final TObjectIntMap<String> words;
	private int totalWordCount;
	private final Bigram bigram;
	private final String parentDatabaseId;
	
	LargeFollowingWordSet(SmallFollowingWordSet set) {
		this.totalWordCount = 0;
		this.words = TCollections.synchronizedMap(new TObjectIntHashMap<String>(set.size() * 11 / 8, 0.8f));
		synchronized(set.getRawWords()) {
			for(String word : set) {
				this.addWord(word);
				this.incrementTotal();
			}
		}
		this.bigram = set.getBigram();
		this.parentDatabaseId = set.getId();
	}
	
	LargeFollowingWordSet(TObjectIntMap<String> map, int wordCount, Bigram bigram, String id) {
		this.words = map;
		this.totalWordCount = wordCount;
		this.bigram = bigram;
		this.parentDatabaseId = id;
	}

	@Override
	public void addWord(String word) {
		this.words.adjustOrPutValue(word, 1, 1);
		this.incrementTotal();
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
			for(TObjectIntIterator<String> iterator = this.words.iterator(); iterator.hasNext();) {
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
	
	/*
	 * O(1) for this implementation of FollowingWordSet
	 */
	@Override
	public boolean contains(String followingWord) {
		return this.words.containsKey(followingWord);
	}
	
	@Override
	public boolean contains(String followingWord, int count) {
		return (this.words.get(followingWord) >= count);
	}

	@Override
	public boolean remove(String followingWord) {
		boolean result = this.words.adjustValue(followingWord, -1);
		int newValue = this.words.get(followingWord);
		if(result) {
			//followingWord was found and adjusted down 1. check if it's now 0
			if(newValue == 0) {
				//new value is 0, so removed followingWord from set
				int removedValue = this.words.remove(followingWord);
				//check for race condition in case this value was modified at the same time
				if(removedValue != 0) {
					//something added a value at the same time. put amount back in
					this.words.adjustOrPutValue(followingWord, removedValue, removedValue);
				}
			}
			this.decrementTotal();
		}
		return result;
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
	
	public int numEntries() {
		return this.words.size();
	}
	
	private void incrementTotal() {
		this.totalWordCount++;
	}
	
	private void decrementTotal() {
		this.totalWordCount--;
	}
	
//	/*
//	 * note we need this to propagate thrown IOExceptions, and any thrown IOExceptions 
//	 * will be rethrown as UncheckedIOException() via the IOProcedure wrapper
//	 */
//	boolean forEachEntry(IOProcedure<String> procedure) {
//		return this.words.forEachEntry(procedure);
//	}

	

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
		if (!(obj instanceof LargeFollowingWordSet))
			return false;
		LargeFollowingWordSet other = (LargeFollowingWordSet) obj;
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
		builder.append("LargeFollowingWordSet [words=");
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

	@Override
	public Type getType() {
		return FollowingWordSet.Type.LARGE;
	}

	@Override
	public void writeToOutput(FSTObjectOutput out) throws IOException {
		out.writeInt(this.getType().getValue());
		out.writeInt(this.numEntries());
		
		synchronized(this.words) {
			for(TObjectIntIterator<String> iterator = this.words.iterator(); iterator.hasNext();) {
				iterator.advance();
				out.writeUTF(iterator.key());
				out.writeInt(iterator.value());
			}
		}
	}

	

}
