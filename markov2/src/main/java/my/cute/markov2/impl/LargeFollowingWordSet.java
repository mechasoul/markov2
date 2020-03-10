package my.cute.markov2.impl;

import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.nustaq.serialization.FSTObjectOutput;
import org.nustaq.serialization.annotations.Flat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gnu.trove.TCollections;
import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;

/*
 * FollowingWordSet implementation for large sets (commonly used bigrams)
 * small maps (small, tiny) use a list and every time a word is used it's added
 * to the list, so a word can occur many times in the list if it's repeated.
 * in this class, instead of just recording each raw instance of a word's use, use a 
 * map of words-> occurrences. for sets where words are frequently repeated, the 
 * maintenance cost of the map is outweighed by the memory saved from replacing 
 * multiple instances of the same string in a list (iirc from testing/examination 
 * breakpoint was around 4 average uses of any unique word in the list). larger sets
 * typically repeat elements more frequently, so this implementation is used for 
 * sufficiently large sets (not always more efficient - eg a set with 1000 unique
 * words each used once would be stored as a map where each entry's key is a word
 * and value is 1, adding memory/time overhead for no gain - but in application
 * this is very uncommon and this approach saves space a large enough majority of
 * the time to be valuable)
 * note that this implementation has O(1) time for contains() and remove(), but
 * O(n) time for getRandomWeightedWord()
 */
@Flat
class LargeFollowingWordSet implements FollowingWordSet, Serializable {
	
	private static final Logger logger = LoggerFactory.getLogger(LargeFollowingWordSet.class);
	private static final long serialVersionUID = 1L;
	/*
	 * actual data of the set - map of words to their frequencies
	 * using trove because its specialized maps are more efficient
	 * than typical jdk implementations
	 */
	private final TObjectIntMap<String> words;
	/*
	 * the total size of the set
	 */
	private int totalWordCount;
	
	private final Bigram bigram;
	private final String parentDatabaseId;
	
	/*
	 * constructor used when building from a small followingwordset. used
	 * when a small fws has reached the threshold to be converted to large
	 */
	LargeFollowingWordSet(SmallFollowingWordSet set) {
		this.totalWordCount = 0;
		this.words = TCollections.synchronizedMap(new TObjectIntHashMap<String>(set.size() * 11 / 8, 0.8f));
		//set uses a synchronized wrapper on arraylist - must manually synchronize on it when iterating
		synchronized(set.getWords()) {
			for(String word : set) {
				this.addWord(word);
			}
		}
		this.bigram = set.getBigram();
		this.parentDatabaseId = set.getId();
	}
	
	/*
	 * constructor used with all data given. used during deserialization
	 */
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
		String chosenWord = null;
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
		//should never be null, since sum of word count over all entries should equal totalWordCount
		if(chosenWord == null) {
			logger.warn(this + ": getRandomWeightedWord() returned null; totalWordCount probably wrong! "
					+ "totalWordCount: " + this.totalWordCount + ", words=" + this.words.toString());
			chosenWord = "hello";
		}
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
		builder.append("LargeFollowingWordSet [bigram=");
		builder.append(bigram);
		builder.append(", parentDatabaseId=");
		builder.append(parentDatabaseId);
		builder.append("]");
		return builder.toString();
	}

	@Override
	public String toStringPlain() {
		/*
		 * add all entries to a list and sort it before printing
		 * sort alphabetically by the followingword
		 */
		List<SimpleImmutableEntry<String, Integer>> entries = new ArrayList<>(this.words.size());
		synchronized(this.words) {
			this.words.forEachEntry((word, count) ->
			{
				entries.add(new SimpleImmutableEntry<>(word, count));
				return true;
			});
		}
		
		Collections.sort(entries, ((first, second) -> 
		{
			return first.getKey().compareTo(second.getKey());
		}));
		
		StringBuilder sb = new StringBuilder("{");
		boolean first = true;
		for(SimpleImmutableEntry<String, Integer> entry : entries) {
			if(first) first = false;
			else sb.append(",");
			
			sb.append(entry.getKey()).append("=").append(entry.getValue());
		}
		sb.append("}");
		return sb.toString();
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

	@Override
	public List<String> getWords() {
		List<String> wordsAsList = new ArrayList<String>(this.totalWordCount);
		synchronized(this.words) {
			for(TObjectIntIterator<String> iterator = this.words.iterator(); iterator.hasNext();) {
				iterator.advance();
				for(int i=0; i < iterator.value(); i++) {
					wordsAsList.add(iterator.key());
				}
			}
		}
		return wordsAsList;
	}	

}
